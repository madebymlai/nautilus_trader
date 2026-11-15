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

use std::{collections::VecDeque, hash::Hash};

use ahash::{AHashMap, AHashSet};
use nautilus_core::{UUID4, UnixNanos};
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
use crate::{
    messages::data::{DataResponse, DataResponseEnvelope, RequestCommand},
    warmup::{Delivered, same_bar, same_quote, same_trade},
};

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
/// Each held series records the requests holding it, what their responses delivered, and its items
/// numbered by arrival, which orders a release across series. A release drops the items the
/// responses already delivered. Completed requests queue their items in `released` until the actor
/// delivers them, so clearing also discards items not yet delivered.
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
            self.released.extend(hold.release().map(|(_, data)| data));
            return WarmupHold::Released;
        }

        held.push((self.arrivals, data));
        hold.bytes += bytes;
        self.arrivals += 1;
        WarmupHold::Held
    }

    /// Records what `response` delivered of its held series, and releases the series a completed
    /// request held once no other pending request holds them.
    pub(super) fn complete(&mut self, response: &DataResponseEnvelope) {
        let request_id = response.correlation_id();
        let Some(series) = self.requests.get(request_id) else {
            return;
        };

        if let Some(payload) = response.response() {
            record_delivered(&mut self.holds, series, payload);
        }

        if !response.completed() {
            return;
        }

        let Some(series) = self.requests.remove(request_id) else {
            return;
        };

        let mut released = Vec::new();

        for series in series {
            let hold = hold_of(&mut self.holds, &series);
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

    /// Returns whether this live item repeats one of the items `delivered` at its `ts_event`.
    ///
    /// Book data matches by venue sequence where set and otherwise by content, a trade by trade
    /// ID, and other data by content, all without `ts_init`. A delivered book holds each update up
    /// to its sequence, a high-water mark as in [`OrderBook`]. A bar confirmed with other values
    /// than the delivered bar still forming at that time is not a repeat.
    fn repeats(&self, delivered: &[Self]) -> bool {
        if let Self::BookDeltas(batch) = self {
            return batch.deltas.iter().all(|live| {
                live.ts_event < batch.ts_event
                    || delivered.iter().any(|item| match item {
                        Self::BookDeltas(deltas) => deltas.deltas.iter().any(|delta| {
                            if live.sequence == 0 {
                                OrderBookDelta {
                                    ts_init: delta.ts_init,
                                    ..*live
                                } == *delta
                            } else {
                                live.sequence == delta.sequence
                            }
                        }),
                        Self::Book(book) => batch.sequence != 0 && batch.sequence <= book.sequence,
                        _ => false,
                    })
            });
        }

        delivered.iter().any(|item| match (self, item) {
            (Self::Quote(live), Self::Quote(quote)) => same_quote(live, quote),
            (Self::Trade(live), Self::Trade(trade)) => same_trade(live, trade),
            (Self::Bar(live), Self::Bar(bar)) => same_bar(live, bar),
            (Self::BookDepth(live), Self::BookDepth(depth)) if live.sequence == 0 => {
                OrderBookDepth {
                    ts_init: depth.ts_init,
                    ..(**live).clone()
                } == **depth
            }
            (Self::BookDepth(live), Self::BookDepth(depth)) => live.sequence == depth.sequence,
            (Self::BookDepth(live), Self::Book(book)) => {
                live.sequence != 0 && live.sequence <= book.sequence
            }
            (Self::Book(live), Self::Book(book)) if live.sequence == 0 => {
                live.bids_as_map(None) == book.bids_as_map(None)
                    && live.asks_as_map(None) == book.asks_as_map(None)
            }
            (Self::Book(live), Self::Book(book)) => live.sequence <= book.sequence,
            (Self::FundingRate(live), Self::FundingRate(rate)) => live == rate,
            (Self::Data(live), Self::Data(data)) => live.data.eq_ignore_ts_init(data.data.as_ref()),
            _ => false,
        })
    }

    fn ts_event(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_event,
            Self::Trade(trade) => trade.ts_event,
            Self::Bar(bar) => bar.ts_event,
            Self::BookDeltas(deltas) => deltas.ts_event,
            Self::BookDepth(depth) => depth.ts_event,
            Self::Book(book) => book.ts_last,
            Self::FundingRate(rate) => rate.ts_event,
            Self::Data(data) => data.data.ts_event(),
        }
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

/// The requests holding one series, what their responses delivered of it, and its items numbered
/// by arrival with their known bytes.
///
/// The items are `None` once the series released them at a limit.
#[derive(Clone, Debug)]
struct SeriesHold {
    requests: Vec<UUID4>,
    delivered: Delivered<WarmupData>,
    held: Option<Vec<(u64, WarmupData)>>,
    bytes: usize,
}

impl SeriesHold {
    fn new() -> Self {
        Self {
            requests: Vec::new(),
            delivered: Delivered::default(),
            held: Some(Vec::new()),
            bytes: 0,
        }
    }

    /// Ends the hold and returns, in arrival order, its items the responses have not delivered.
    fn release(&mut self) -> impl Iterator<Item = (u64, WarmupData)> {
        self.bytes = 0;
        let held = self.held.take().unwrap_or_default();
        held.into_iter().filter(|(_, data)| {
            !self
                .delivered
                .includes(data.ts_event(), |items| data.repeats(items))
        })
    }
}

/// Records what `response` delivered of the `series` a request holds, at each one's latest
/// `ts_event`.
///
/// A book snapshot delivers its instrument's deltas, depth and book up to its last update.
fn record_delivered(
    holds: &mut AHashMap<WarmupSeries, SeriesHold>,
    series: &[WarmupSeries],
    response: &DataResponse,
) {
    match response {
        DataResponse::Quotes(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::Quotes(id) => Some(*id),
                _ => None,
            },
            &r.data,
            |quote| quote.instrument_id,
            |quote| quote.ts_event,
            |quote| WarmupData::Quote(*quote),
        ),
        DataResponse::Trades(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::Trades(id) => Some(*id),
                _ => None,
            },
            &r.data,
            |trade| trade.instrument_id,
            |trade| trade.ts_event,
            |trade| WarmupData::Trade(*trade),
        ),
        DataResponse::Bars(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::Bars(bar_type) => Some(*bar_type),
                _ => None,
            },
            &r.data,
            |bar| bar.bar_type.standard(),
            |bar| bar.ts_event,
            |bar| WarmupData::Bar(*bar),
        ),
        DataResponse::BookDeltas(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::BookDeltas(id) => Some(*id),
                _ => None,
            },
            &r.data,
            |delta| delta.instrument_id,
            |delta| delta.ts_event,
            |delta| WarmupData::BookDeltas(OrderBookDeltas::new(delta.instrument_id, vec![*delta])),
        ),
        DataResponse::BookDepth(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::BookDepth(id) => Some(*id),
                _ => None,
            },
            &r.data,
            |depth| depth.instrument_id,
            |depth| depth.ts_event,
            |depth| WarmupData::BookDepth(Box::new(depth.clone())),
        ),
        DataResponse::FundingRates(r) => record_latest(
            holds,
            series,
            |series| match series {
                WarmupSeries::FundingRates(id) => Some(*id),
                _ => None,
            },
            &r.data,
            |rate| rate.instrument_id,
            |rate| rate.ts_event,
            |rate| WarmupData::FundingRate(*rate),
        ),
        DataResponse::Book(r) => {
            let book = &r.data;

            for series in series {
                if let WarmupSeries::BookDeltas(id)
                | WarmupSeries::BookDepth(id)
                | WarmupSeries::Book(id) = series
                    && *id == book.instrument_id
                {
                    let delivered = &mut hold_of(holds, series).delivered;
                    delivered.record(book.ts_last, [WarmupData::Book(book.clone())]);
                }
            }
        }
        DataResponse::Data(r) => {
            let items = match r.data.downcast_ref::<Vec<CustomData>>() {
                Some(items) => items.as_slice(),
                None => r.data.downcast_ref().map_or_default(std::slice::from_ref),
            };

            // Data types are equal when their topics are
            record_latest(
                holds,
                series,
                |series| match series {
                    WarmupSeries::Data(data_type) => Some(data_type.topic()),
                    _ => None,
                },
                items,
                |item| item.data_type.topic(),
                |item| item.data.ts_event(),
                |item| WarmupData::Data(item.clone()),
            );
        }
        _ => {}
    }
}

/// Records the latest of `items` for each of `series` that `series_key` names, in one pass that
/// assigns each item its series by `item_key` and copies only the items at each latest time.
fn record_latest<'a, T, K: Eq + Hash>(
    holds: &mut AHashMap<WarmupSeries, SeriesHold>,
    series: &'a [WarmupSeries],
    series_key: impl Fn(&'a WarmupSeries) -> Option<K>,
    items: &'a [T],
    item_key: impl Fn(&'a T) -> K,
    ts_event: impl Fn(&T) -> UnixNanos,
    data: impl Fn(&T) -> WarmupData,
) {
    let mut latest: AHashMap<K, (&WarmupSeries, UnixNanos, Vec<&T>)> = series
        .iter()
        .filter_map(|series| {
            Some((
                series_key(series)?,
                (series, UnixNanos::default(), Vec::new()),
            ))
        })
        .collect();

    for item in items {
        let Some((_, latest_ts, latest_items)) = latest.get_mut(&item_key(item)) else {
            continue;
        };
        let ts = ts_event(item);

        if ts > *latest_ts {
            *latest_ts = ts;
            latest_items.clear();
        }

        if ts == *latest_ts {
            latest_items.push(item);
        }
    }

    for (series, ts, items) in latest.into_values() {
        hold_of(holds, series)
            .delivered
            .record(ts, items.into_iter().map(&data));
    }
}

fn hold_of<'a>(
    holds: &'a mut AHashMap<WarmupSeries, SeriesHold>,
    series: &WarmupSeries,
) -> &'a mut SeriesHold {
    holds
        .get_mut(series)
        .expect("Invariant: a pending request's series is held")
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
        data::{
            BookOrder,
            depth::{DEPTH_INLINE_LEN, DEPTH10_LEN},
            stubs::{stub_custom_data, stub_depth10, stub_remote_data},
        },
        enums::{BookType, OrderSide},
        identifiers::{ClientId, TradeId},
        orderbook::level::BookLevel,
        types::{Price, Quantity},
    };
    use proptest::prelude::*;
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use smallvec::SmallVec;

    use super::{
        super::storage,
        WarmupSeries::{Bars, Book, BookDeltas, BookDepth, Data, Quotes},
        *,
    };
    use crate::messages::data::{
        CustomDataResponse, DataResponse, QuotesResponse, RequestBars, RequestBookSnapshot,
        RequestCustomData, RequestInstrument, RequestQuotes,
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
        stub_custom_data(0, 0, None, None).data_type
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

    fn tick(instrument_id: InstrumentId, ts: u64, bid: u32) -> QuoteTick {
        QuoteTick {
            instrument_id,
            bid_price: Price::new(f64::from(bid), 0),
            ts_event: UnixNanos::from(ts),
            ..Default::default()
        }
    }

    fn quote(instrument_id: InstrumentId, ts: u64) -> WarmupData {
        WarmupData::Quote(tick(instrument_id, ts, 0))
    }

    fn trade(trade_id: &str, ts_init: u64) -> WarmupData {
        WarmupData::Trade(TradeTick {
            trade_id: TradeId::new(trade_id),
            ts_init: UnixNanos::from(ts_init),
            ..Default::default()
        })
    }

    fn bar(close: u32, volume: u32, ts_init: u64) -> WarmupData {
        typed_bar(bar_type(), close, volume, ts_init)
    }

    fn typed_bar(bar_type: BarType, close: u32, volume: u32, ts_init: u64) -> WarmupData {
        WarmupData::Bar(Bar::new(
            bar_type,
            Price::new(100.0, 0),
            Price::new(105.0, 0),
            Price::new(99.0, 0),
            Price::new(f64::from(close), 0),
            Quantity::from(volume),
            20.into(),
            ts_init.into(),
        ))
    }

    fn delta(sequence: u64, flags: u8, ts_init: u64) -> OrderBookDelta {
        let delta = OrderBookDelta::clear(aud(), sequence, 20.into(), ts_init.into());
        OrderBookDelta { flags, ..delta }
    }

    fn deltas(sequence: u64, flags: u8, ts_init: u64) -> WarmupData {
        let delta = delta(sequence, flags, ts_init);
        WarmupData::BookDeltas(OrderBookDeltas::new(aud(), vec![delta]))
    }

    fn depth(sequence: u64, ts_init: u64) -> WarmupData {
        WarmupData::BookDepth(Box::new(OrderBookDepth {
            sequence,
            ts_init: UnixNanos::from(ts_init),
            ..stub_depth10()
        }))
    }

    fn depth_counted(count: u32, ts_init: u64) -> WarmupData {
        WarmupData::BookDepth(Box::new(OrderBookDepth {
            bid_counts: [count; DEPTH10_LEN].into(),
            ts_init: UnixNanos::from(ts_init),
            ..stub_depth10()
        }))
    }

    fn funding(rate: rust_decimal::Decimal) -> WarmupData {
        let rate = FundingRateUpdate::new(aud(), rate, None, None, 0.into(), 0.into());
        WarmupData::FundingRate(rate)
    }

    fn book(sequence: u64) -> WarmupData {
        let mut book = OrderBook::new(aud(), BookType::L2_MBP);
        book.sequence = sequence;
        WarmupData::Book(book)
    }

    fn book_bid(price: u32) -> WarmupData {
        let mut book = OrderBook::new(aud(), BookType::L2_MBP);
        let order = BookOrder::new(
            OrderSide::Buy,
            Price::new(f64::from(price), 0),
            Quantity::from(1),
            0,
        );
        book.add(order, 0, 0, UnixNanos::from(20));
        WarmupData::Book(book)
    }

    fn custom(price: &str, ts_init: u64) -> WarmupData {
        WarmupData::Data(stub_remote_data(price, 20, ts_init))
    }

    fn window(
        command: &RequestCommand,
        quotes: &[QuoteTick],
        completed: bool,
    ) -> DataResponseEnvelope {
        let response = QuotesResponse::new(
            *command.request_id(),
            ClientId::new("HISTORY"),
            aud(),
            quotes.to_vec(),
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
        warmup.complete(&window(&first, &[], false));
        let after_window = released(&mut warmup);
        warmup.complete(&window(&first, &[], true));
        let after_first = released(&mut warmup);
        warmup.complete(&window(&second, &[], true));
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
    fn test_response_of_several_instruments_marks_each_series_by_its_own_items() {
        let request = quotes(&[aud(), gbp()], Some(update(true)));
        let mut warmup = ActorWarmup::default();
        warmup.open(&request);
        let live = [
            (aud(), 20, 1),
            (gbp(), 20, 1),
            (aud(), 22, 0),
            (gbp(), 25, 1),
            (gbp(), 30, 0),
        ];

        for (id, ts, bid) in live {
            warmup.hold(|| Quotes(id), || WarmupData::Quote(tick(id, ts, bid)));
        }

        // The last AUD quote is at 20 and the last GBP quote at 25
        let delivered = [tick(aud(), 10, 0), tick(gbp(), 25, 2), tick(aud(), 20, 1)];
        warmup.complete(&window(&request, &delivered, true));

        assert_eq!(
            released(&mut warmup),
            [(aud(), 22), (gbp(), 25), (gbp(), 30)]
        );
    }

    #[rstest]
    fn test_single_custom_data_response_marks_its_item_delivered() {
        let request = data(Some(update(true)));
        let mut warmup = ActorWarmup::default();
        warmup.open(&request);
        let live = |ts| WarmupData::Data(stub_custom_data(ts, 0, None, None));
        warmup.hold(|| Data(data_type()), || live(10));
        warmup.hold(|| Data(data_type()), || live(30));

        let response = CustomDataResponse::new(
            *request.request_id(),
            ClientId::new("CUSTOM"),
            None,
            data_type(),
            stub_custom_data(20, 0, None, None),
            None,
            None,
            0.into(),
            None,
        );
        warmup.complete(&DataResponseEnvelope::new(
            DataResponse::Data(response),
            true,
            None,
        ));
        let released: Vec<_> = std::iter::from_fn(|| warmup.next_released())
            .map(|data| data.ts_event().as_u64())
            .collect();

        assert_eq!(released, [30]);
    }

    #[rstest]
    #[case::quote_resent(
        WarmupData::Quote(QuoteTick { ts_init: 9.into(), ..tick(aud(), 20, 0) }),
        vec![quote(aud(), 20)],
        true,
    )]
    #[case::quote_distinct(WarmupData::Quote(tick(aud(), 20, 1)), vec![quote(aud(), 20)], false)]
    #[case::trade_resent(trade("T-1", 5), vec![trade("T-1", 9)], true)]
    #[case::trade_distinct(trade("T-1", 5), vec![trade("T-2", 5)], false)]
    #[case::bar_resent(bar(101, 2, 5), vec![bar(101, 2, 9)], true)]
    #[case::bar_close_distinct(bar(102, 2, 5), vec![bar(101, 2, 5)], false)]
    #[case::bar_volume_distinct(bar(101, 3, 5), vec![bar(101, 2, 5)], false)]
    #[case::bar_resent_composite(
        typed_bar(internal_bar_type(), 101, 2, 5),
        vec![typed_bar(composite_bar_type(), 101, 2, 9)],
        true,
    )]
    #[case::bar_type_distinct(
        typed_bar(internal_bar_type(), 101, 2, 5),
        vec![bar(101, 2, 5)],
        false,
    )]
    #[case::deltas_sequence(deltas(7, 0, 5), vec![deltas(8, 0, 5)], false)]
    #[case::deltas_sequence_other_flags(deltas(7, 1, 5), vec![deltas(7, 0, 9)], true)]
    #[case::deltas_batch_partly_delivered(
        WarmupData::BookDeltas(OrderBookDeltas::new(aud(), vec![delta(0, 1, 5), delta(0, 0, 5)])),
        vec![deltas(0, 0, 9)],
        false,
    )]
    #[case::deltas_content(deltas(0, 0, 5), vec![deltas(0, 0, 9)], true)]
    #[case::deltas_content_distinct(deltas(0, 1, 5), vec![deltas(0, 0, 5)], false)]
    #[case::deltas_earlier_in_batch(
        WarmupData::BookDeltas(OrderBookDeltas::new(
            aud(),
            vec![OrderBookDelta { ts_event: 10.into(), ..delta(0, 1, 5) }, delta(0, 0, 5)],
        )),
        vec![deltas(0, 0, 9)],
        true,
    )]
    #[case::deltas_in_snapshot(deltas(7, 0, 5), vec![book(7)], true)]
    #[case::deltas_before_snapshot(deltas(6, 0, 5), vec![book(7)], true)]
    #[case::deltas_after_snapshot(deltas(8, 0, 5), vec![book(7)], false)]
    #[case::deltas_unsequenced_against_snapshot(deltas(0, 0, 5), vec![book(0)], false)]
    #[case::depth_sequence(depth(7, 5), vec![depth(8, 5)], false)]
    #[case::depth_content(depth(0, 5), vec![depth(0, 9)], true)]
    #[case::depth_content_distinct(depth_counted(3, 5), vec![depth_counted(4, 9)], false)]
    #[case::depth_in_snapshot(depth(7, 5), vec![book(7)], true)]
    #[case::depth_before_snapshot(depth(6, 5), vec![book(7)], true)]
    #[case::depth_after_snapshot(depth(8, 5), vec![book(7)], false)]
    #[case::depth_unsequenced_against_snapshot(depth(0, 5), vec![book(0)], false)]
    #[case::book_before_snapshot(book(6), vec![book(7)], true)]
    #[case::book_after_snapshot(book(8), vec![book(7)], false)]
    #[case::book_content(book_bid(1), vec![book_bid(1)], true)]
    #[case::book_content_distinct(book_bid(1), vec![book_bid(2)], false)]
    #[case::funding_distinct(funding(dec!(0.01)), vec![funding(dec!(0.02))], false)]
    #[case::custom_resent(custom("1.00", 5), vec![custom("1.00", 9)], true)]
    #[case::custom_distinct(custom("1.00", 5), vec![custom("2.00", 5)], false)]
    fn test_live_item_repeats_delivered_item(
        #[case] live: WarmupData,
        #[case] delivered: Vec<WarmupData>,
        #[case] expected: bool,
    ) {
        assert_eq!(live.repeats(&delivered), expected);
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

    #[derive(Clone, Debug)]
    enum Event {
        Live(u64, u32),
        Window(Vec<(u64, u32)>),
    }

    proptest! {
        #[rstest]
        fn prop_release_is_held_items_not_delivered_in_arrival_order(
            events in prop::collection::vec(
                prop_oneof![
                    (0..20_u64, 0..3_u32).prop_map(|(ts, bid)| Event::Live(ts, bid)),
                    prop::collection::vec((0..20_u64, 0..3_u32), 0..3).prop_map(Event::Window),
                ],
                0..30,
            ),
            last in prop::collection::vec((0..20_u64, 0..3_u32), 0..3),
        ) {
            let request = quotes(&[aud()], Some(update(true)));
            let mut warmup = ActorWarmup::default();
            warmup.open(&request);
            let ticks = |items: &[(u64, u32)]| -> Vec<QuoteTick> {
                items.iter().map(|(ts, bid)| tick(aud(), *ts, *bid)).collect()
            };

            for event in &events {
                match event {
                    Event::Live(ts, bid) => {
                        warmup.hold(|| Quotes(aud()), || WarmupData::Quote(tick(aud(), *ts, *bid)));
                    }
                    Event::Window(items) => {
                        warmup.complete(&window(&request, &ticks(items), false));
                    }
                }
            }
            warmup.complete(&window(&request, &ticks(&last), true));

            let delivered: Vec<(u64, u32)> = events
                .iter()
                .filter_map(|event| match event {
                    Event::Window(items) => Some(items.clone()),
                    Event::Live(..) => None,
                })
                .flatten()
                .chain(last)
                .collect();
            let last_ts = delivered.iter().map(|(ts, _)| *ts).max();
            let expected: Vec<_> = events
                .iter()
                .filter_map(|event| match event {
                    Event::Live(ts, bid)
                        if last_ts.is_none_or(|last| *ts > last)
                            || (Some(*ts) == last_ts && !delivered.contains(&(*ts, *bid))) =>
                    {
                        Some((aud(), *ts))
                    }
                    _ => None,
                })
                .collect();
            prop_assert_eq!(released(&mut warmup), expected);
        }
    }
}
