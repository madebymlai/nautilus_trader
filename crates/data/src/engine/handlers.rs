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

use std::{cell::RefCell, rc::Rc};

use ahash::AHashMap;
use nautilus_common::msgbus::Handler;
use nautilus_core::{UnixNanos, WeakCell};
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};
use ustr::Ustr;

use super::bar::BarAggregatorWarmup;
use crate::aggregation::{BarAggregator, SpreadQuoteAggregator};

// Between built-in handlers (10) and default user handlers (0)
pub(super) const BAR_AGGREGATOR_PRIORITY: u32 = 5;

/// Holds live data back from a bar aggregator that is still replaying a historical request.
///
/// The buffer collects while it is `Some` and is emptied by [`WarmupBuffer::release`], after which
/// the handlers holding it pass data straight through again.
#[derive(Debug)]
pub(super) struct WarmupBuffer<T>(Rc<RefCell<Option<WarmupState<T>>>>);

/// The live data held during a warm-up, and the historical items at the last historical `ts_event`.
///
/// Each historical item drops one held copy of itself, which leaves `None` in its place in `held`,
/// found through `held_by_ts_event`. The items at the last historical `ts_event` stay recorded, so
/// a copy that arrives after them, as when a live stream replays its latest item, is dropped too.
#[derive(Debug)]
struct WarmupState<T> {
    held: Vec<Option<T>>,
    held_by_ts_event: AHashMap<UnixNanos, Vec<usize>>,
    history_end: Option<(UnixNanos, Vec<T>)>,
}

/// Data a [`WarmupBuffer`] can match against the history it is replayed with.
pub(super) trait WarmupData: Copy {
    fn ts_event(&self) -> UnixNanos;

    /// Returns whether `other`, at the same `ts_event`, is another copy of the same venue item.
    fn is_copy_of(&self, other: &Self) -> bool;

    /// Returns the buffer in `warmup` that holds this data type, if any.
    fn warmup_buffer(warmup: &BarAggregatorWarmup) -> Option<&WarmupBuffer<Self>>;
}

impl WarmupData for QuoteTick {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn is_copy_of(&self, other: &Self) -> bool {
        self.instrument_id == other.instrument_id
            && self.bid_price == other.bid_price
            && self.ask_price == other.ask_price
            && self.bid_size == other.bid_size
            && self.ask_size == other.ask_size
    }

    fn warmup_buffer(warmup: &BarAggregatorWarmup) -> Option<&WarmupBuffer<Self>> {
        match warmup {
            BarAggregatorWarmup::Quote(buffer) => Some(buffer),
            _ => None,
        }
    }
}

impl WarmupData for TradeTick {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn is_copy_of(&self, other: &Self) -> bool {
        self.instrument_id == other.instrument_id && self.trade_id == other.trade_id
    }

    fn warmup_buffer(warmup: &BarAggregatorWarmup) -> Option<&WarmupBuffer<Self>> {
        match warmup {
            BarAggregatorWarmup::Trade(buffer) => Some(buffer),
            _ => None,
        }
    }
}

impl WarmupData for Bar {
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    // A bar type closes one bar at each `ts_event`
    fn is_copy_of(&self, other: &Self) -> bool {
        self.bar_type == other.bar_type
    }

    fn warmup_buffer(warmup: &BarAggregatorWarmup) -> Option<&WarmupBuffer<Self>> {
        match warmup {
            BarAggregatorWarmup::Bar(buffer) => Some(buffer),
            _ => None,
        }
    }
}

impl<T> Clone for WarmupBuffer<T> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<T: WarmupData> WarmupBuffer<T> {
    pub(super) fn new() -> Self {
        Self(Rc::new(RefCell::new(Some(WarmupState {
            held: Vec::new(),
            held_by_ts_event: AHashMap::new(),
            history_end: None,
        }))))
    }

    /// Holds `item` and returns `true` while warming up, otherwise returns `false`.
    ///
    /// A copy of a historical item at the last historical `ts_event` is dropped instead.
    fn hold(&self, item: &T) -> bool {
        let mut warmup = self.0.borrow_mut();
        let Some(warmup) = warmup.as_mut() else {
            return false;
        };

        let ts_event = item.ts_event();

        if let Some((last, delivered)) = &mut warmup.history_end
            && ts_event == *last
            && let Some(index) = delivered.iter().position(|d| d.is_copy_of(item))
        {
            delivered.swap_remove(index);
            return true;
        }

        warmup
            .held_by_ts_event
            .entry(ts_event)
            .or_default()
            .push(warmup.held.len());
        warmup.held.push(Some(*item));
        true
    }

    /// Drops the held data and keeps holding.
    pub(super) fn drop_held(&self) {
        if let Some(warmup) = self.0.borrow_mut().as_mut() {
            warmup.held.clear();
            warmup.held_by_ts_event.clear();
        }
    }

    /// Records a historical `item` replayed while warming up, dropping one held copy of it.
    pub(super) fn record_history(&self, item: &T) {
        let mut warmup = self.0.borrow_mut();
        let Some(warmup) = warmup.as_mut() else {
            return;
        };

        let ts_event = item.ts_event();

        match &mut warmup.history_end {
            Some((last, items)) if ts_event == *last => items.push(*item),
            Some((last, items)) if ts_event > *last => {
                *last = ts_event;
                items.clear();
                items.push(*item);
            }
            Some(_) => {}
            None => warmup.history_end = Some((ts_event, vec![*item])),
        }

        if let Some(indices) = warmup.held_by_ts_event.get_mut(&ts_event)
            && let Some(position) = indices
                .iter()
                .position(|&index| warmup.held[index].is_some_and(|held| held.is_copy_of(item)))
        {
            warmup.held[indices.swap_remove(position)] = None;
        }
    }

    /// Ends the warm-up and returns the held data the history did not deliver, in arrival order.
    pub(super) fn release(&self) -> Vec<T> {
        self.0.borrow_mut().take().map_or_else(Vec::new, |warmup| {
            warmup.held.into_iter().flatten().collect()
        })
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
    fn test_warmup_release_drops_held_bar_for_a_historical_close() {
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
        let revised = bar("1.00005", 60_500);
        let newer = Bar {
            ts_event: UnixNanos::from(120_000),
            ..revised
        };

        for item in [revised, newer] {
            assert!(warmup.hold(&item));
        }
        warmup.record_history(&bar("1.00000", 60_000));

        assert_eq!(warmup.release(), vec![newer]);
    }

    #[rstest]
    fn test_warmup_drops_copy_of_history_end_held_after_it() {
        let warmup = WarmupBuffer::new();
        warmup.record_history(&quote("1.00000", 1_000, 1_000));
        warmup.record_history(&quote("1.00000", 2_000, 2_000));
        let newer = quote("1.00001", 3_000, 3_000);

        assert!(warmup.hold(&quote("1.00000", 2_000, 2_500)));
        assert!(warmup.hold(&newer));
        assert_eq!(warmup.release(), vec![newer]);
    }

    #[rstest]
    fn test_warmup_release_keeps_held_data_without_history() {
        let warmup = WarmupBuffer::new();
        let held = quote("1.00000", 1_000, 1_000);
        assert!(warmup.hold(&held));

        assert_eq!(warmup.release(), vec![held]);
    }
}
