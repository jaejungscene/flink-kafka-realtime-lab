CREATE TABLE IF NOT EXISTS merchant_risk_profiles (
  merchant_id TEXT PRIMARY KEY,
  risk_tier TEXT NOT NULL,
  risk_multiplier NUMERIC(6, 3) NOT NULL,
  manual_review_required BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO merchant_risk_profiles (merchant_id, risk_tier, risk_multiplier, manual_review_required)
VALUES
  ('merchant-hot', 'HIGH', 1.700, TRUE),
  ('merchant-01', 'LOW', 0.900, FALSE),
  ('merchant-07', 'MEDIUM', 1.200, FALSE),
  ('merchant-12', 'MEDIUM', 1.150, FALSE)
ON CONFLICT (merchant_id) DO UPDATE
SET
  risk_tier = EXCLUDED.risk_tier,
  risk_multiplier = EXCLUDED.risk_multiplier,
  manual_review_required = EXCLUDED.manual_review_required,
  updated_at = now();

ALTER TABLE merchant_risk_profiles REPLICA IDENTITY FULL;
