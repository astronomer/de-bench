-- An RMA is amended after it is raised, so the merge keys on `rma_id`.
INSERT OR REPLACE INTO copperline.raw.returns BY NAME
SELECT * FROM copperline.staging.returns_landing
