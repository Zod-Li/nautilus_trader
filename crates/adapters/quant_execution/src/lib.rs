// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! In-memory $O(1)$ `PlanGate` admission engine and async write-behind telemetry queue
//! for [NautilusTrader](https://nautechsystems.io).

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod engine;
pub mod models;

#[cfg(feature = "python")]
pub mod python;

pub use engine::RustPlanGateEngine;
pub use models::{RustPlanGateDecision, RustPlanGateOrderRequest, RustPlanItemEnvelope};
