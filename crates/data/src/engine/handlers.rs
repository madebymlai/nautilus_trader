// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{cell::RefCell, cmp::Ordering, rc::Rc};

use nautilus_common::msgbus::Handler;
use nautilus_core::{UnixNanos, WeakCell};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};
use ustr::Ustr;

use crate::aggregation::{BarAggregator, SpreadQuoteAggregator};

// Between built-in handlers (10) and default user handlers (0)
pub(super) const BAR_AGGREGATOR_PRIORITY: u32 = 5;

/// Holds live data back from a bar aggregator that is still replaying a historical request.
///
/// The buffer collects while it is `Some` and is emptied by [`WarmupBuffer::release`], after which
/// the handlers holding it pass data straight through again.
#[derive(Debug)]
pub(super) struct WarmupBuffer<T>(Rc<RefCell<Option<Warmup<T>>>>);

#[derive(Debug)]
struct Warmup<T> {
    held: Vec<T>,
    // The last historical `ts_event` and the historical items at it
    history_end: Option<(UnixNanos, Vec<T>)>,
}

/// Data a [`WarmupBuffer`] can match against the historical data it replays after.
pub(super) trait WarmupData: Copy {
    fn ts_event(&self) -> UnixNanos;

    /// Returns whether `other` is another copy of the same venue item, whatever its `ts_init`.
    fn is_same(&self, other: &Self) -> bool;
}

impl WarmupData for QuoteTick {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn is_same(&self, other: &Self) -> bool {
        self.instrument_id == other.instrument_id
            && self.bid_price == other.bid_price
            && self.ask_price == other.ask_price
            && self.bid_size == other.bid_size
            && self.ask_size == other.ask_size
            && self.ts_event == other.ts_event
    }
}

impl WarmupData for TradeTick {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn is_same(&self, other: &Self) -> bool {
        self.instrument_id == other.instrument_id && self.trade_id == other.trade_id
    }
}

impl WarmupData for Bar {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn is_same(&self, other: &Self) -> bool {
        self.bar_type == other.bar_type
            && self.open == other.open
            && self.high == other.high
            && self.low == other.low
            && self.close == other.close
            && self.volume == other.volume
            && self.ts_event == other.ts_event
    }
}

impl<T> Clone for WarmupBuffer<T> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<T: WarmupData> WarmupBuffer<T> {
    pub(super) fn new() -> Self {
        Self(Rc::new(RefCell::new(Some(Warmup {
            held: Vec::new(),
            history_end: None,
        }))))
    }

    /// Holds `item` and returns `true` while warming up, otherwise returns `false`.
    fn hold(&self, item: &T) -> bool {
        match self.0.borrow_mut().as_mut() {
            Some(warmup) => {
                warmup.held.push(*item);
                true
            }
            None => false,
        }
    }

    /// Records a historical `item` replayed while warming up.
    pub(super) fn record_history(&self, item: &T) {
        let mut warmup = self.0.borrow_mut();
        let Some(warmup) = warmup.as_mut() else {
            return;
        };
        let ts_event = item.ts_event();

        match &mut warmup.history_end {
            Some((last, items)) if ts_event == *last => items.push(*item),
            Some((last, _)) if ts_event < *last => {}
            history_end => *history_end = Some((ts_event, vec![*item])),
        }
    }

    /// Ends the warm-up and returns the held data the history did not deliver, in arrival order.
    ///
    /// Held data before the last historical `ts_event` is dropped, and at that `ts_event` each
    /// historical item drops one held copy of itself.
    pub(super) fn release(&self) -> Vec<T> {
        let Some(Warmup { held, history_end }) = self.0.borrow_mut().take() else {
            return Vec::new();
        };
        let Some((last, mut delivered)) = history_end else {
            return held;
        };

        held.into_iter()
            .filter(|item| match item.ts_event().cmp(&last) {
                Ordering::Less => false,
                Ordering::Greater => true,
                Ordering::Equal => match delivered.iter().position(|d| d.is_same(item)) {
                    Some(index) => {
                        delivered.swap_remove(index);
                        false
                    }
                    None => true,
                },
            })
            .collect()
    }
}

/// Message handler for processing quote ticks through bar aggregators.
///
/// This handler receives quote tick messages and forwards them to the underlying
/// bar aggregator for processing. It's used as part of the data engine's message
/// routing infrastructure to build bars from incoming quote data.
#[derive(Debug)]
pub(super) struct BarQuoteHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: Option<WarmupBuffer<QuoteTick>>,
}

impl BarQuoteHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<QuoteTick>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<QuoteTick> for BarQuoteHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarQuoteHandler|{}", self.bar_type))
    }

    fn handle(&self, quote: &QuoteTick) {
        if self
            .warmup
            .as_ref()
            .is_some_and(|warmup| warmup.hold(quote))
        {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_quote(*quote);
        }
    }
}

/// Message handler for processing trade ticks through bar aggregators.
///
/// This handler receives trade tick messages and forwards them to the underlying
/// bar aggregator for processing. It's used as part of the data engine's message
/// routing infrastructure to build bars from incoming trade data.
#[derive(Debug)]
pub(super) struct BarTradeHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: Option<WarmupBuffer<TradeTick>>,
}

impl BarTradeHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<TradeTick>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<TradeTick> for BarTradeHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarTradeHandler|{}", self.bar_type))
    }

    fn handle(&self, trade: &TradeTick) {
        if self
            .warmup
            .as_ref()
            .is_some_and(|warmup| warmup.hold(trade))
        {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_trade(*trade);
        }
    }
}

/// Message handler for processing bars through composite bar aggregators.
///
/// This handler receives bar messages and forwards them to the underlying
/// bar aggregator for further processing. It's used for building composite
/// bars from existing bars, such as creating higher timeframe bars from
/// lower timeframe bars.
#[derive(Debug)]
pub(super) struct BarBarHandler {
    aggregator: WeakCell<Box<dyn BarAggregator>>,
    bar_type: BarType,
    warmup: Option<WarmupBuffer<Bar>>,
}

impl BarBarHandler {
    pub(crate) fn new(aggregator: &Rc<RefCell<Box<dyn BarAggregator>>>, bar_type: BarType) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            bar_type,
            warmup: None,
        }
    }

    /// Holds incoming data in `warmup` until the buffer is released.
    pub(super) fn with_warmup(mut self, warmup: Option<WarmupBuffer<Bar>>) -> Self {
        self.warmup = warmup;
        self
    }
}

impl Handler<Bar> for BarBarHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!("BarBarHandler|{}", self.bar_type))
    }

    fn handle(&self, bar: &Bar) {
        if self.warmup.as_ref().is_some_and(|warmup| warmup.hold(bar)) {
            return;
        }

        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_bar(*bar);
        }
    }
}

/// Message handler for processing leg quotes through spread quote aggregators.
#[derive(Debug)]
pub(super) struct SpreadQuoteHandler {
    aggregator: WeakCell<SpreadQuoteAggregator>,
    spread_instrument_id: InstrumentId,
    leg_instrument_id: InstrumentId,
}

impl SpreadQuoteHandler {
    pub(crate) fn new(
        aggregator: &Rc<RefCell<SpreadQuoteAggregator>>,
        spread_instrument_id: InstrumentId,
        leg_instrument_id: InstrumentId,
    ) -> Self {
        Self {
            aggregator: WeakCell::from(Rc::downgrade(aggregator)),
            spread_instrument_id,
            leg_instrument_id,
        }
    }
}

impl Handler<QuoteTick> for SpreadQuoteHandler {
    fn id(&self) -> Ustr {
        Ustr::from(&format!(
            "SpreadQuoteHandler|{}|{}",
            self.spread_instrument_id, self.leg_instrument_id,
        ))
    }

    fn handle(&self, quote: &QuoteTick) {
        if let Some(agg) = self.aggregator.upgrade() {
            agg.borrow_mut().handle_quote_tick(*quote);
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::types::{Price, Quantity};
    use rstest::rstest;

    use super::*;

    fn quote(bid: &str, ts_event: u64, ts_init: u64) -> QuoteTick {
        QuoteTick::new(
            InstrumentId::from("AUD/USD.SIM"),
            Price::from(bid),
            Price::from("1.00010"),
            Quantity::from("1000"),
            Quantity::from("1000"),
            UnixNanos::from(ts_event),
            UnixNanos::from(ts_init),
        )
    }

    #[rstest]
    fn test_warmup_release_drops_one_held_copy_per_historical_quote() {
        let warmup = WarmupBuffer::new();
        let held = [
            quote("1.00000", 1_000, 1_500),
            quote("1.00000", 2_000, 2_500),
            quote("1.00000", 2_000, 2_600),
            quote("1.00001", 2_000, 2_700),
            quote("1.00002", 3_000, 3_000),
        ];

        for item in &held {
            assert!(warmup.hold(item));
        }
        warmup.record_history(&quote("1.00000", 1_000, 1_000));
        warmup.record_history(&quote("1.00000", 2_000, 2_000));

        assert_eq!(warmup.release(), vec![held[2], held[3], held[4]]);
        assert!(!warmup.hold(&held[0]));
    }

    #[rstest]
    fn test_warmup_release_drops_held_copy_of_historical_bar() {
        let bar_type = BarType::from("AUD/USD.SIM-1-MINUTE-BID-EXTERNAL");
        let bar = |close: &str, ts_init: u64| {
            Bar::new(
                bar_type,
                Price::from("1.00000"),
                Price::from("1.00010"),
                Price::from("0.99990"),
                Price::from(close),
                Quantity::from("1000"),
                UnixNanos::from(60_000),
                UnixNanos::from(ts_init),
            )
        };
        let warmup = WarmupBuffer::new();
        let held = [bar("1.00000", 60_500), bar("1.00005", 60_500)];

        for item in &held {
            assert!(warmup.hold(item));
        }
        warmup.record_history(&bar("1.00000", 60_000));

        assert_eq!(warmup.release(), vec![held[1]]);
    }

    #[rstest]
    fn test_warmup_release_keeps_held_data_without_history() {
        let warmup = WarmupBuffer::new();
        let held = quote("1.00000", 1_000, 1_000);
        assert!(warmup.hold(&held));

        assert_eq!(warmup.release(), vec![held]);
    }
}
