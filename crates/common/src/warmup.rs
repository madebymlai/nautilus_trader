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

//! What a historical request delivered, by which a warmup drops the live data it held meanwhile.

use nautilus_core::UnixNanos;
use nautilus_model::data::{Bar, QuoteTick, TradeTick};

/// The items a request delivered at the latest `ts_event` of one series.
///
/// A held live item before that time was delivered. One at that time was delivered only when it
/// repeats a recorded item, since distinct items can share a coarse venue timestamp.
#[derive(Clone, Debug)]
pub struct Delivered<T> {
    ts_event: UnixNanos,
    items: Vec<T>,
}

impl<T> Default for Delivered<T> {
    fn default() -> Self {
        Self {
            ts_event: UnixNanos::default(),
            items: Vec::new(),
        }
    }
}

impl<T> Delivered<T> {
    /// Records `items`, delivered at `ts_event`, reading them only if no later time is recorded.
    pub fn record(&mut self, ts_event: UnixNanos, items: impl IntoIterator<Item = T>) {
        if ts_event > self.ts_event {
            self.ts_event = ts_event;
            self.items.clear();
        }

        if ts_event == self.ts_event {
            self.items.extend(items);
        }
    }

    /// Returns whether a live item at `ts_event` was delivered, where `repeats` tells whether it
    /// repeats one of the items recorded at that time.
    pub fn includes(&self, ts_event: UnixNanos, repeats: impl FnOnce(&[T]) -> bool) -> bool {
        ts_event < self.ts_event || (ts_event == self.ts_event && repeats(&self.items))
    }
}

/// Returns whether two quotes are copies of one venue quote, which differ only in `ts_init`.
pub fn same_quote(a: &QuoteTick, b: &QuoteTick) -> bool {
    QuoteTick {
        ts_init: b.ts_init,
        ..*a
    } == *b
}

/// Returns whether two bars are copies of one venue bar, which differ only in `ts_init` and in
/// whether their bar type is composite or the standard type it outputs.
///
/// A venue's confirmed bar copies the bar it served still forming at that time only when their
/// values agree.
pub fn same_bar(a: &Bar, b: &Bar) -> bool {
    a.bar_type.standard() == b.bar_type.standard()
        && Bar {
            bar_type: b.bar_type,
            ts_init: b.ts_init,
            ..*a
        } == *b
}

/// Returns whether two trades are copies of one venue trade, which its trade ID identifies.
pub fn same_trade(a: &TradeTick, b: &TradeTick) -> bool {
    a.trade_id == b.trade_id
}
