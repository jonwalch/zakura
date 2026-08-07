//! Async Sapling batch verifier service

use core::fmt;
use std::{
    collections::HashSet,
    future::Future,
    mem,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use once_cell::sync::Lazy;
use rand::thread_rng;
use tokio::sync::watch;
use tower::{util::ServiceFn, Service};
use tower_batch_control::{Batch, BatchControl, RequestWeight};
use tower_fallback::Fallback;

use sapling_crypto::{bundle::Authorized, BatchValidator, Bundle};
use zakura_chain::transaction::SigHash;
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::value::ZatBalance;

use crate::{error::TransactionError, BoxError};

/// Sapling prover containing spend and output params for the Sapling circuit.
///
/// Used to:
///
/// - construct Sapling outputs in coinbase txs, and
/// - verify Sapling shielded data in the tx verifier.
static SAPLING: Lazy<LocalTxProver> = Lazy::new(LocalTxProver::bundled);

/// Returns the process-wide Sapling prover for constructing Sapling proofs, initializing it on
/// first use.
///
/// The bundled Sapling spend and output proving parameters are parsed once, then the same prover is
/// reused for proof construction and verification for the lifetime of the process.
pub fn sapling_prover() -> &'static LocalTxProver {
    Lazy::force(&SAPLING)
}

#[derive(Clone)]
pub struct Item {
    /// The bundle containing the Sapling shielded data to verify.
    bundle: Bundle<Authorized, ZatBalance>,
    /// The sighash of the transaction that contains the Sapling shielded data.
    sighash: SigHash,
}

impl Item {
    /// Creates a new [`Item`] from a Sapling bundle and sighash.
    pub fn new(bundle: Bundle<Authorized, ZatBalance>, sighash: SigHash) -> Self {
        Self { bundle, sighash }
    }
}

impl RequestWeight for Item {
    fn request_weight(&self) -> usize {
        self.bundle
            .shielded_spends()
            .len()
            .saturating_add(self.bundle.shielded_outputs().len())
    }
}

/// A service that verifies Sapling shielded data in batches.
///
/// Handles batching incoming requests, driving batches to completion, and reporting results.
#[derive(Default)]
pub struct Verifier {
    /// A batch verifier for Sapling shielded data.
    batch: BatchValidator,

    /// A channel for broadcasting the verification result of the batch.
    ///
    /// Each batch gets a newly created channel, so there is only ever one result sent per channel.
    /// Tokio doesn't have a oneshot multi-consumer channel, so we use a watch channel.
    tx: watch::Sender<Option<bool>>,
}

impl fmt::Debug for Verifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Verifier")
            .field("batch", &"..")
            .field("tx", &self.tx)
            .finish()
    }
}

impl Drop for Verifier {
    // Flush the current batch in case there are still any pending futures.
    //
    // Flushing the batch means we need to validate it. This function fires off the validation and
    // returns immediately, usually before the validation finishes.
    fn drop(&mut self) {
        let batch = mem::take(&mut self.batch);
        let tx = mem::take(&mut self.tx);

        // The validation is CPU-intensive; do it on a dedicated thread so it does not block.
        rayon::spawn_fifo(move || {
            let (spend_vk, output_vk) = SAPLING.verifying_keys();

            // Validate the batch and send the result through the channel.
            let res = batch.validate(&spend_vk, &output_vk, thread_rng());
            let _ = tx.send(Some(res));
        });
    }
}

impl Service<BatchControl<Item>> for Verifier {
    type Response = ();
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: BatchControl<Item>) -> Self::Future {
        match req {
            BatchControl::Item(item) => {
                let mut rx = self.tx.subscribe();

                let bundle_check = self
                    .batch
                    .check_bundle(item.bundle, item.sighash.into())
                    .then_some(())
                    .ok_or(TransactionError::SaplingVerificationFailed);

                async move {
                    bundle_check.map_err(BoxError::from)?;

                    rx.changed()
                        .await
                        .map_err(|_| BoxError::from("verifier was dropped without flushing"))?;

                    // We use a new channel for each batch, so we always get the correct
                    // batch result here.
                    let is_valid = *rx.borrow().as_ref().ok_or_else(|| {
                        Box::<dyn std::error::Error + Send + Sync>::from(
                            "threadpool unexpectedly dropped channel sender",
                        )
                    })?;

                    if is_valid {
                        metrics::counter!("proofs.sapling.verified").increment(1);
                        Ok(())
                    } else {
                        metrics::counter!("proofs.sapling.invalid").increment(1);
                        Err(BoxError::from(TransactionError::SaplingVerificationFailed))
                    }
                }
                .boxed()
            }

            BatchControl::Flush => {
                let batch = mem::take(&mut self.batch);
                let tx = mem::take(&mut self.tx);

                async move {
                    let start = std::time::Instant::now();
                    let spawn_result = tokio::task::spawn_blocking(move || {
                        let (spend_vk, output_vk) = SAPLING.verifying_keys();
                        batch.validate(&spend_vk, &output_vk, thread_rng())
                    })
                    .await;
                    let duration = start.elapsed().as_secs_f64();

                    let result_label = match &spawn_result {
                        Ok(true) => "success",
                        _ => "failure",
                    };
                    metrics::histogram!(
                        "zakura.consensus.batch.duration_seconds",
                        "verifier" => "groth16_sapling",
                        "result" => result_label
                    )
                    .record(duration);

                    // Extract the value before consuming spawn_result
                    let is_valid = spawn_result.as_ref().ok().copied();
                    let _ = tx.send(is_valid);
                    spawn_result.map(|_| ()).map_err(Self::Error::from)
                }
                .boxed()
            }
        }
    }
}

/// Verifies a single [`Item`].
pub fn verify_single(
    item: Item,
) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send>> {
    async move {
        let mut verifier = Verifier::default();

        let check = verifier
            .batch
            .check_bundle(item.bundle, item.sighash.into())
            .then_some(())
            .ok_or(TransactionError::SaplingVerificationFailed);
        check.map_err(BoxError::from)?;

        let is_valid = tokio::task::spawn_blocking(move || {
            let (spend_vk, output_vk) = SAPLING.verifying_keys();

            mem::take(&mut verifier.batch).validate(&spend_vk, &output_vk, thread_rng())
        })
        .await
        .map_err(|_| BoxError::from("Sapling bundle validation thread panicked"))?;

        if is_valid {
            Ok(())
        } else {
            Err(BoxError::from(TransactionError::SaplingVerificationFailed))
        }
    }
    .boxed()
}

/// The batch-and-fallback stack that actually verifies Sapling bundles.
type SaplingBatchFallback = Fallback<
    Batch<Verifier, Item>,
    ServiceFn<fn(Item) -> BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
>;

/// Global batch verification context for Sapling shielded data.
pub static VERIFIER: Lazy<BenchMemoized<SaplingBatchFallback>> = Lazy::new(|| {
    BenchMemoized::new(Fallback::new(
        Batch::new(
            Verifier::default(),
            super::MAX_BATCH_SIZE,
            None,
            super::MAX_BATCH_LATENCY,
        ),
        tower::service_fn(verify_single),
    ))
});

// ---------------------------------------------------------------------------
// NOT FOR MERGE — measurement prototype only.
//
// This exists to answer one question: what is the *ceiling* on what a Sapling memo could
// buy? It is off unless `ZAKURA_BENCH_ENABLE_SAPLING_MEMO=1`, so the default build behaves
// exactly as it did before.
//
// It is deliberately not production-quality. The key encoder below is hand-written, and a
// hand-written encoder that silently omits a field is precisely the failure mode that makes
// a memo unsafe — see the module docs on `primitives::halo2::memo`. Shipping this would
// require the encoder to come from `zcash_primitives` (whose `write_v5_bundle` for Sapling
// is `pub(crate)`), plus a discriminant separating the v4 per-spend-anchor encoding from
// the v5 shared-anchor one. Neither is done here, because neither changes the timing.
// ---------------------------------------------------------------------------

/// Whether the prototype Sapling memo is active. Read once.
static BENCH_ENABLE_SAPLING_MEMO: Lazy<bool> = Lazy::new(|| {
    std::env::var("ZAKURA_BENCH_ENABLE_SAPLING_MEMO")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
});

/// Personalization for the prototype Sapling memo key.
const SAPLING_MEMO_PERSONALIZATION: &[u8; 16] = b"ZakuraSapngMemo1";

impl Item {
    /// Derives a memo key committing to this bundle's encoding and its sighash.
    ///
    /// See the NOT FOR MERGE banner above: this encoder is for cost measurement, not for
    /// consensus use.
    fn bench_cache_key(&self) -> [u8; 32] {
        let Item { bundle, sighash } = self;

        let mut hasher = blake2b_simd::Params::new()
            .hash_length(32)
            .personal(SAPLING_MEMO_PERSONALIZATION)
            .to_state();

        hasher.update(&sighash.0);

        for spend in bundle.shielded_spends() {
            hasher.update(&spend.cv().to_bytes());
            hasher.update(&spend.anchor().to_bytes());
            hasher.update(spend.nullifier().as_ref());
            hasher.update(&<[u8; 32]>::from(*spend.rk()));
            hasher.update(spend.zkproof());
            hasher.update(&<[u8; 64]>::from(*spend.spend_auth_sig()));
        }

        for output in bundle.shielded_outputs() {
            hasher.update(&output.cv().to_bytes());
            hasher.update(&output.cmu().to_bytes());
            hasher.update(output.ephemeral_key().as_ref());
            hasher.update(output.enc_ciphertext());
            hasher.update(output.out_ciphertext());
            hasher.update(output.zkproof());
        }

        hasher.update(&i64::from(*bundle.value_balance()).to_le_bytes());
        hasher.update(&<[u8; 64]>::from(bundle.authorization().binding_sig));

        hasher
            .finalize()
            .as_bytes()
            .try_into()
            .expect("hash_length(32) produces exactly 32 bytes")
    }
}

/// A prototype memo over Sapling bundle verification, mirroring the Halo2 one.
pub struct BenchMemoized<S> {
    inner: S,
    verified: Arc<Mutex<HashSet<[u8; 32]>>>,
}

impl<S: Clone> Clone for BenchMemoized<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            verified: self.verified.clone(),
        }
    }
}

impl<S> BenchMemoized<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            verified: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Returns the wrapped verification stack.
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S> Service<Item> for BenchMemoized<S>
where
    S: Service<Item, Response = (), Error = BoxError>,
    S::Future: Send + 'static,
{
    type Response = ();
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<(), BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, item: Item) -> Self::Future {
        if !*BENCH_ENABLE_SAPLING_MEMO {
            return self.inner.call(item).boxed();
        }

        let key = item.bench_cache_key();

        if self
            .verified
            .lock()
            .expect("prototype sapling memo mutex should not be poisoned")
            .contains(&key)
        {
            metrics::counter!("zakura.consensus.sapling.bench_memo.hit").increment(1);
            return std::future::ready(Ok(())).boxed();
        }

        metrics::counter!("zakura.consensus.sapling.bench_memo.miss").increment(1);

        let verified = self.verified.clone();
        let response = self.inner.call(item);

        async move {
            let result = response.await;

            if result.is_ok() {
                verified
                    .lock()
                    .expect("prototype sapling memo mutex should not be poisoned")
                    .insert(key);
            }

            result
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::sapling_prover;

    #[test]
    fn sapling_prover_is_reused() {
        assert!(std::ptr::eq(sapling_prover(), sapling_prover()));
    }
}
