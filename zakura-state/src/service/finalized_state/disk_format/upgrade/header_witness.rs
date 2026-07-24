//! Rebases header-root authentication so missing terminal witnesses are refetched.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

use super::{header_root_auth_frontier::rebase_to_body_tip, CancelFormatChange, DiskFormatUpgrade};

/// First format that durably retains the terminal header witness.
pub(crate) const UPGRADE_VERSION: Version = Version::new(28, 0, 3);

/// Repairs databases that advanced their root frontier without retaining its witness.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        UPGRADE_VERSION
    }

    fn description(&self) -> &'static str {
        "rebase header-root authentication to refetch its terminal header witness"
    }

    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZakuraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        check_cancelled(cancel_receiver)?;
        if let Err(error) = rebase_to_body_tip(db) {
            panic!("header-witness recovery failed closed: {error}");
        }
        check_cancelled(cancel_receiver)?;
        Ok(())
    }

    fn validate(
        &self,
        db: &ZakuraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        match db.validate_header_root_auth_state() {
            Ok(_) => Ok(Ok(())),
            Err(error) => Ok(Err(error.to_string())),
        }
    }
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}
