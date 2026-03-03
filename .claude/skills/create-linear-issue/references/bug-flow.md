# Bug Flow

## Step 1: Investigation

Spawn an `Explore` sub-agent to review code and recent git history. It returns the standard split:

**INTERNAL (coordinator only):** likely root cause, affected scope, recent relevant commits, estimated fix complexity (XS/S/M/L).

**FOR THE ISSUE:** user-visible symptom, reproduction steps, severity, frequency.

## Step 2: Present Findings

Show the user the product-level findings. Ask to confirm or correct.

## Step 3: Estimate

Use the sub-agent's internal scope signal.
