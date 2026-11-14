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

use std::collections::VecDeque;

use ahash::{AHashMap, AHashSet};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, FundingRateUpdate, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth, QuoteTick, TradeTick,
    },
    identifiers::InstrumentId,
    orderbook::OrderBook,
};

use super::{
    dispatch::{MAX_KNOWN_BYTES, MAX_PENDING},
    storage,
};
use crate::messages::data::{DataResponseEnvelope, RequestCommand};

/// The most live items one series holds before releasing them ahead of its requests.
///
/// With high precision, a held item takes 256 bytes plus any book data it owns, so a full series of
/// quotes takes 16 MiB, about a minute of a feed at 1,000 updates per second. Matches the callback
/// queue's count limit.
pub(super) const MAX_HELD: usize = MAX_PENDING;

/// The most known bytes one series holds before releasing them ahead of its requests.
///
/// With high precision, quotes, trades, bars and funding rates reach [`MAX_HELD`] first, at 16 MiB,
/// while book data reaches this limit: about 32,000 depth snapshots, or 150 copies of a book of
/// 1,000 levels a side. Matches the callback queue's known-storage limit.
pub(super) const MAX_HELD_BYTES: usize = MAX_KNOWN_BYTES;

/// The live data an actor holds for its pending `update_subscriptions` requests.
///
/// Each held series records the requests holding it and its items numbered by arrival, which
/// orders a release across series. Completed requests queue their items in `released` until the
/// actor delivers them, so clearing also discards items not yet delivered.
#[derive(Clone, Debug, Default)]
pub(super) struct ActorWarmup {
    holds: AHashMap<WarmupSeries, SeriesHold>,
    requests: AHashMap<UUID4, Vec<WarmupSeries>>,
    released: VecDeque<WarmupData>,
    arrivals: u64,
}

impl ActorWarmup {
    /// Opens a hold on each series `command` requests when its params set `update_subscriptions`.
    pub(super) fn open(&mut self, command: &RequestCommand) {
        let request_id = *command.request_id();
        let series = request_series(command);
        if series.is_empty() {
            return;
        }

        for series in &series {
            let hold = self
                .holds
                .entry(series.clone())
                .or_insert_with(SeriesHold::new);
            hold.requests.push(request_id);
        }

        self.requests.insert(request_id, series);
    }

    /// Holds the live item `data` builds when a pending request holds its `series`.
    ///
    /// Builds the key only while some request holds data, and the item only when its series is
    /// held, since a custom data key clones its data type and a book item its book. A series
    /// whose next item would exceed [`MAX_HELD`] items or [`MAX_HELD_BYTES`] releases them
    /// instead and passes until its requests complete.
    pub(super) fn hold(
        &mut self,
        series: impl FnOnce() -> WarmupSeries,
        data: impl FnOnce() -> WarmupData,
    ) -> WarmupHold {
        if self.holds.is_empty() {
            return WarmupHold::Pass;
        }

        let series = series();
        let Some(hold) = self.holds.get_mut(&series) else {
            return WarmupHold::Pass;
        };
        let Some(held) = hold.held.as_mut() else {
            return WarmupHold::Pass;
        };

        let data = data();
        let bytes = data.known_bytes();

        if held.len() >= MAX_HELD || bytes > MAX_HELD_BYTES - hold.bytes {
            log::warn!(
                "Held {} live items of {series:?} in {} bytes for requests {:?}, \
                 releasing them before the requests complete",
                held.len(),
                hold.bytes,
                hold.requests,
            );
            let released = hold.release().into_iter().map(|(_, data)| data);
            self.released.extend(released);
            return WarmupHold::Released;
        }

        held.push((self.arrivals, data));
        hold.bytes += bytes;
        self.arrivals += 1;
        WarmupHold::Held
    }

    /// Releases the series a completed request held once no other pending request holds them.
    pub(super) fn complete(&mut self, response: &DataResponseEnvelope) {
        let request_id = response.correlation_id();
        if !response.completed() {
            return;
        }

        let Some(series) = self.requests.remove(request_id) else {
            return;
        };

        let mut released = Vec::new();

        for series in series {
            let hold = self
                .holds
                .get_mut(&series)
                .expect("Invariant: a pending request's series is held");
            hold.requests.retain(|id| id != request_id);

            if hold.requests.is_empty() {
                released.extend(hold.release());
                self.holds.remove(&series);
            }
        }

        released.sort_unstable_by_key(|(arrival, _)| *arrival);
        self.released
            .extend(released.into_iter().map(|(_, data)| data));
    }

    pub(super) fn next_released(&mut self) -> Option<WarmupData> {
        self.released.pop_front()
    }

    pub(super) fn clear(&mut self) {
        *self = Self::default();
    }
}

/// What [`ActorWarmup::hold`] did with a live item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum WarmupHold {
    /// No pending request holds the item's series.
    Pass,
    /// A pending request holds the item.
    Held,
    /// The series reached a limit and released its items, to deliver before this one.
    Released,
}

/// A live item an `update_subscriptions` request can hold.
#[derive(Clone, Debug)]
pub(super) enum WarmupData {
    Quote(QuoteTick),
    Trade(TradeTick),
    Bar(Bar),
    BookDeltas(OrderBookDeltas),
    BookDepth(Box<OrderBookDepth>),
    Book(OrderBook),
    FundingRate(FundingRateUpdate),
    Data(CustomData),
}

impl WarmupData {
    /// Returns the bytes this item takes when held: its slot, and the book levels, deltas, depth
    /// and data type it owns, measured as the callback queue measures them.
    fn known_bytes(&self) -> usize {
        let owned = match self {
            Self::BookDeltas(deltas) => deltas
                .deltas
                .capacity()
                .saturating_mul(size_of::<OrderBookDelta>()),
            Self::BookDepth(depth) => {
                size_of::<OrderBookDepth>().saturating_add(storage::depth(depth))
            }
            Self::Book(book) => storage::book(book),
            Self::Data(data) => storage::custom_data(data),
            Self::Quote(_) | Self::Trade(_) | Self::Bar(_) | Self::FundingRate(_) => 0,
        };
        size_of::<(u64, Self)>().saturating_add(owned)
    }
}

/// A data type for one identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum WarmupSeries {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Bars(BarType),
    BookDeltas(InstrumentId),
    BookDepth(InstrumentId),
    Book(InstrumentId),
    FundingRates(InstrumentId),
    Data(DataType),
}

/// The requests holding one series, and its items numbered by arrival with their known bytes.
///
/// The items are `None` once the series released them at a limit.
#[derive(Clone, Debug)]
struct SeriesHold {
    requests: Vec<UUID4>,
    held: Option<Vec<(u64, WarmupData)>>,
    bytes: usize,
}

impl SeriesHold {
    fn new() -> Self {
        Self {
            requests: Vec::new(),
            held: Some(Vec::new()),
            bytes: 0,
        }
    }

    /// Ends the hold and returns its items in arrival order.
    fn release(&mut self) -> Vec<(u64, WarmupData)> {
        self.bytes = 0;
        self.held.take().unwrap_or_default()
    }
}

fn request_series(command: &RequestCommand) -> Vec<WarmupSeries> {
    let each = |ids: &[InstrumentId], series: fn(InstrumentId) -> WarmupSeries| {
        ids.iter().copied().map(series).collect::<Vec<_>>()
    };

    let (params, series) = match command {
        RequestCommand::Data(cmd) => (&cmd.params, vec![WarmupSeries::Data(cmd.data_type.clone())]),
        RequestCommand::BookSnapshot(cmd) => {
            let book = |id| {
                [
                    WarmupSeries::BookDeltas(id),
                    WarmupSeries::BookDepth(id),
                    WarmupSeries::Book(id),
                ]
            };
            (
                &cmd.params,
                cmd.instrument_ids.iter().copied().flat_map(book).collect(),
            )
        }
        RequestCommand::BookDeltas(cmd) => (
            &cmd.params,
            each(&cmd.instrument_ids, WarmupSeries::BookDeltas),
        ),
        RequestCommand::BookDepth(cmd) => (
            &cmd.params,
            each(&cmd.instrument_ids, WarmupSeries::BookDepth),
        ),
        RequestCommand::Quotes(cmd) => {
            (&cmd.params, each(&cmd.instrument_ids, WarmupSeries::Quotes))
        }
        RequestCommand::Trades(cmd) => {
            (&cmd.params, each(&cmd.instrument_ids, WarmupSeries::Trades))
        }
        RequestCommand::FundingRates(cmd) => (
            &cmd.params,
            each(&cmd.instrument_ids, WarmupSeries::FundingRates),
        ),
        RequestCommand::Bars(cmd) => {
            // Live bars arrive under their standard type, so types sharing one hold one series
            let mut standard = AHashSet::new();
            let series = cmd
                .bar_types
                .iter()
                .map(BarType::standard)
                .filter(|bar_type| standard.insert(*bar_type))
                .map(WarmupSeries::Bars)
                .collect();
            (&cmd.params, series)
        }
        // Instrument definitions have no time order to hold
        RequestCommand::Instrument(_)
        | RequestCommand::Instruments(_)
        | RequestCommand::OptionChainReferencePrice(_)
        | RequestCommand::Join(_) => return Vec::new(),
    };

    let key = params
        .as_ref()
        .and_then(|params| params.get_bool("update_subscriptions"));
    if key == Some(true) {
        series
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{Params, UnixNanos};
    use nautilus_model::{
        data::{BookOrder, depth::DEPTH_INLINE_LEN, stubs::stub_depth10},
        enums::{BookType, OrderSide},
        identifiers::ClientId,
        orderbook::level::BookLevel,
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use smallvec::SmallVec;

    use super::{
        super::storage,
        WarmupSeries::{Bars, Book, BookDeltas, BookDepth, Data, Quotes},
        *,
    };
    use crate::messages::data::{
        DataResponse, QuotesResponse, RequestBars, RequestBookSnapshot, RequestCustomData,
        RequestInstrument, RequestQuotes,
    };

    fn aud() -> InstrumentId {
        InstrumentId::from("AUD/USD.SIM")
    }

    fn gbp() -> InstrumentId {
        InstrumentId::from("GBP/USD.SIM")
    }

    fn eur() -> InstrumentId {
        InstrumentId::from("EUR/USD.SIM")
    }

    fn bar_type() -> BarType {
        BarType::from("AUD/USD.SIM-1-MINUTE-LAST-EXTERNAL")
    }

    fn composite_bar_type() -> BarType {
        BarType::from("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL")
    }

    // The standard type of `composite_bar_type`
    fn internal_bar_type() -> BarType {
        BarType::from("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL")
    }

    fn data_type() -> DataType {
        DataType::new("Custom", None, None)
    }

    fn params(key: &str, value: bool) -> Params {
        Params::new([(key.to_string(), serde_json::json!(value))].into())
    }

    fn update(value: bool) -> Params {
        params("update_subscriptions", value)
    }

    fn quotes(ids: &[InstrumentId], params: Option<Params>) -> RequestCommand {
        let request =
            RequestQuotes::new(ids, None, None, None, None, UUID4::new(), 0.into(), params);
        RequestCommand::Quotes(request)
    }

    fn book_snapshot(params: Option<Params>) -> RequestCommand {
        let request = RequestBookSnapshot::new(aud(), None, None, UUID4::new(), 0.into(), params);
        RequestCommand::BookSnapshot(request)
    }

    fn bars(bar_types: &[BarType], params: Option<Params>) -> RequestCommand {
        let id = UUID4::new();
        RequestCommand::Bars(RequestBars::new(
            bar_types,
            None,
            None,
            None,
            None,
            id,
            0.into(),
            params,
        ))
    }

    fn data(params: Option<Params>) -> RequestCommand {
        let client_id = ClientId::new("CUSTOM");
        let id = UUID4::new();
        let request = RequestCustomData::new(
            client_id,
            data_type(),
            None,
            None,
            None,
            id,
            0.into(),
            params,
        );
        RequestCommand::Data(request)
    }

    fn instrument(params: Option<Params>) -> RequestCommand {
        let request =
            RequestInstrument::new(aud(), None, None, None, UUID4::new(), 0.into(), params);
        RequestCommand::Instrument(request)
    }

    fn quote(instrument_id: InstrumentId, ts: u64) -> WarmupData {
        WarmupData::Quote(QuoteTick {
            instrument_id,
            ts_event: UnixNanos::from(ts),
            ..Default::default()
        })
    }

    fn window(command: &RequestCommand, completed: bool) -> DataResponseEnvelope {
        let response = QuotesResponse::new(
            *command.request_id(),
            ClientId::new("HISTORY"),
            aud(),
            Vec::new(),
            None,
            None,
            0.into(),
            None,
        );
        DataResponseEnvelope::new(DataResponse::Quotes(response), completed, None)
    }

    fn released(warmup: &mut ActorWarmup) -> Vec<(InstrumentId, u64)> {
        std::iter::from_fn(|| warmup.next_released())
            .map(|data| match data {
                WarmupData::Quote(quote) => (quote.instrument_id, quote.ts_event.as_u64()),
                other => panic!("expected a quote, was {other:?}"),
            })
            .collect()
    }

    // `len` copies of `item` in a heap buffer of `capacity`, beyond a depth side's inline levels
    fn on_heap<T: Clone>(item: T, len: usize, capacity: usize) -> SmallVec<[T; DEPTH_INLINE_LEN]> {
        let mut items = Vec::with_capacity(capacity);
        items.resize(len, item);
        SmallVec::from_vec(items)
    }

    #[rstest]
    #[case::instruments(
        quotes(&[aud(), gbp()], Some(update(true))),
        vec![Quotes(aud()), Quotes(gbp())],
    )]
    #[case::book_snapshot(
        book_snapshot(Some(update(true))),
        vec![BookDeltas(aud()), BookDepth(aud()), Book(aud())],
    )]
    #[case::bars(bars(&[bar_type()], Some(update(true))), vec![Bars(bar_type())])]
    #[case::composite_bars(
        bars(&[composite_bar_type()], Some(update(true))),
        vec![Bars(internal_bar_type())],
    )]
    #[case::bars_of_one_standard_type(
        bars(&[composite_bar_type(), internal_bar_type()], Some(update(true))),
        vec![Bars(internal_bar_type())],
    )]
    #[case::data(data(Some(update(true))), vec![Data(data_type())])]
    #[case::instrument(instrument(Some(update(true))), vec![])]
    #[case::without_params(quotes(&[aud()], None), vec![])]
    #[case::other_params(quotes(&[aud()], Some(params("source", true))), vec![])]
    #[case::key_false(quotes(&[aud()], Some(update(false))), vec![])]
    fn test_request_series(#[case] command: RequestCommand, #[case] expected: Vec<WarmupSeries>) {
        assert_eq!(request_series(&command), expected);
    }

    #[rstest]
    fn test_release_merges_series_in_arrival_order_once_no_request_holds_them() {
        let first = quotes(&[aud(), gbp()], Some(update(true)));
        let second = quotes(&[gbp(), eur()], Some(update(true)));
        let jpy = InstrumentId::from("USD/JPY.SIM");
        let mut warmup = ActorWarmup::default();
        warmup.open(&first);
        warmup.open(&second);

        let holds: Vec<_> = [
            (aud(), 1),
            (gbp(), 2),
            (jpy, 3),
            (eur(), 4),
            (gbp(), 5),
            (eur(), 6),
        ]
        .into_iter()
        .map(|(id, ts)| warmup.hold(|| Quotes(id), || quote(id, ts)))
        .collect();
        warmup.complete(&window(&first, false));
        let after_window = released(&mut warmup);
        warmup.complete(&window(&first, true));
        let after_first = released(&mut warmup);
        warmup.complete(&window(&second, true));
        let after_second = released(&mut warmup);

        assert_eq!(
            holds,
            [
                WarmupHold::Held,
                WarmupHold::Held,
                WarmupHold::Pass,
                WarmupHold::Held,
                WarmupHold::Held,
                WarmupHold::Held,
            ]
        );
        assert_eq!(after_window, []);
        assert_eq!(after_first, [(aud(), 1)]);
        assert_eq!(
            after_second,
            [(gbp(), 2), (eur(), 4), (gbp(), 5), (eur(), 6)]
        );
        assert_eq!(
            warmup.hold(|| Quotes(gbp()), || quote(gbp(), 7)),
            WarmupHold::Pass
        );
    }

    #[rstest]
    fn test_depth_beyond_its_inline_levels_is_charged_its_heap_capacity() {
        let shallow = stub_depth10();
        let (len, capacity) = (2 * DEPTH_INLINE_LEN, 4 * DEPTH_INLINE_LEN);
        let deep = OrderBookDepth {
            bids: on_heap(shallow.bids[0], len, capacity),
            asks: on_heap(shallow.asks[0], len, capacity),
            bid_counts: on_heap(1, len, capacity),
            ask_counts: on_heap(1, len, capacity),
            ..shallow.clone()
        };
        let charge = |depth| WarmupData::BookDepth(Box::new(depth)).known_bytes();

        let heap = 2 * capacity * (size_of::<BookOrder>() + size_of::<u32>());
        assert_eq!(charge(deep), charge(shallow) + heap);
    }

    #[rstest]
    fn test_series_releases_before_its_items_exceed_the_byte_limit() {
        let request = book_snapshot(Some(update(true)));
        let mut warmup = ActorWarmup::default();
        warmup.open(&request);
        let level = size_of::<BookLevel>() + size_of::<(u64, BookOrder)>();
        let levels = u32::try_from(MAX_HELD_BYTES * 2 / 5 / level).unwrap();
        let mut book = OrderBook::new(aud(), BookType::L2_MBP);
        for i in 1..=levels {
            let order = BookOrder::new(
                OrderSide::Buy,
                Price::new(f64::from(i), 0),
                Quantity::from(1),
                u64::from(i),
            );
            book.add(order, 0, u64::from(i), UnixNanos::from(1));
        }
        let book_at = |ts: u64| {
            let mut book = book.clone();
            book.ts_last = UnixNanos::from(ts);
            WarmupData::Book(book)
        };

        let holds: Vec<_> = (1..=4)
            .map(|ts| warmup.hold(|| Book(aud()), || book_at(ts)))
            .collect();
        let released: Vec<_> = std::iter::from_fn(|| warmup.next_released())
            .map(|data| match data {
                WarmupData::Book(book) => book.ts_last.as_u64(),
                other => panic!("expected a book, was {other:?}"),
            })
            .collect();

        // Each book's levels take about two fifths of the limit, so two fit and a third does not
        let known = storage::book(&book);
        assert!((MAX_HELD_BYTES / 3..MAX_HELD_BYTES / 2).contains(&known));
        assert_eq!(
            holds,
            [
                WarmupHold::Held,
                WarmupHold::Held,
                WarmupHold::Released,
                WarmupHold::Pass,
            ]
        );
        assert_eq!(released, [1, 2]);
    }
}
