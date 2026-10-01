-- Backs GET /packages?orderRef= (ADR-0033, PackageRepo.FindByOrderRef):
-- the package read model looks cartons up by their order reference, the
-- same access path migration 0006 indexed for tasks. Non-unique — an order
-- can legitimately produce more than one carton.
CREATE INDEX IF NOT EXISTS idx_packages_order_ref ON packages (order_ref);
