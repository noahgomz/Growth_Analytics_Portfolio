# [BuildMyCreditApp](https://buildmycreditapp.com/) — Credit Decision Engine

A systems dynamics engine that asks the question:

**Given your current profile, budget, and risk tolerance —— what's the best sequence of actions (pay, open, close, use) to maximize your score over time, adjusting as your actions change your profile?**

**Results**: a pilot on 3 users showed **45% improvement in credit profile completeness** and **20% average score increase**.

Built in JavaScript (Node.js) with a Firebase cloud function layer for FlutterFlow app integration. The core engine runs independently of Firebase as a pure logic library.

![Offer](https://github.com/noahgomz/Growth_Analytics_Portfolio/blob/master/Systems%20modeling/BuildMyCreditApp(BMCA)/OfferSnapshot.png)

---

## What It Does

Most credit advice is static: "pay down utilization" or "don't open too many accounts." This engine is dynamic. It treats a user's credit profile as a system of interdependent variables and iterates after each action in a period (1 month) and at the end of that period. 

The engine:
- Ingests a user's real credit report (parsed from upload)
- Models each account as an entity with tracked attributes (balances, limits, ages, payment history)
- Aggregates account-level attributes into global FICO-factor metrics
- Prescribes optimal actions each month using proprietary prioritization logic
- Simulates the effect of those actions on the profile, month by month
- Outputs a ranked action plan with projected profile trajectory

---

## Architecture

The system has three layers:

```
Credit Report Upload
        ↓
[ Parser ] — converts raw report into structured account data
        ↓
[ Credit Engine ] — core logic library (lib/creditEngine)
        ↓
[ Cloud Functions ] — Firebase wrapper exposing engine to app (functions/cf)
        ↓
[ Firebase / FlutterFlow App ] — database, user state, front-end triggers
```

---

## Core Concepts

### 1. Accounts
The atomic unit of the engine. Each account (credit card, installment loan) carries measurable attributes:
- **Count** — number of accounts by type
- **Sum values** — balances, limits, amounts owed
- **Age metrics** — open date, average age, oldest/newest account

### 2. Per-Account Metrics
Computed from account attributes. For example: is this account current? What's the minimum payment? What's the utilization ratio? These feed into the FICO factor computations.

### 3. Global Metrics (FICO Factors)
Aggregations across all accounts, mapped to the 5 FICO factors:
- **Payment History (35%)** — current/late status across all accounts
- **Amounts Owed (30%)** — aggregate utilization and balances
- **Length of History (15%)** — average and oldest account age
- **Credit Mix (10%)** — distribution of revolving vs installment accounts
- **New Credit (10%)** — recent inquiries and new account openings

Each global metric is compared against a high-achiever benchmark to compute a gap score — the optimization target.

### 4. Risk Profile
A user-level input that modulates how aggressively the engine prioritizes actions. Conservative profiles minimize risk of short-term score dips; aggressive profiles pursue maximum long-term gain. This sits above the action layer and shapes the entire decision stack.

### 5. Account Feeders
Recommended cards and loans lists that serve as inputs to open actions. The engine doesn't just tell a user to open an account — it recommends specific products matched to their profile and risk tolerance.

### 6. Actions
Four action types, each modifying the in-memory account state sequentially within each simulated month:

| Action | What It Does |
|--------|-------------|
| **Pay** | Applies budget to accounts using proprietary prioritization logic (min payments first, then surplus allocation) |
| **Open** | Adds a new account from the feeder list, updating mix and history metrics |
| **Close** | Removes an account, with logic to protect age and mix |
| **Use** | Applies estimated spend to revolving accounts, updating utilization |

**Order matters.** Each month runs pay → open → close → use in sequence. The output of each action becomes the input state for the next. This is what makes the engine a systems dynamics model rather than a static calculator.

### 7. Simulation State
An in-memory object that holds the full account and metric state between action cycles. `tickOneMonth()` advances the simulation one period after all four actions have run, aging accounts, updating history, and recomputing all metrics from the new state. The engine iterates this loop across a user-defined planning horizon.

---

## Credit Report Parser
A separate utility that parses real credit report uploads into structured simulation inputs — meaning the engine runs on a user's actual data, not hypothetical inputs. This is what made live pilot testing possible.

---

## File Structure

```
lib/
  creditEngine/
    sim/              # Simulation state, tick logic, output writing
    pay/              # Payment prioritization logic
    open/             # Account opening logic
    close/            # Account closing logic
    use/              # Spend application logic
    helperFunctions/  # Recomputation layer (runs after each action)
    loaders/          # Input handling and data prep

functions/
  cf/                 # 45+ Firebase cloud functions wrapping the engine
    compute*/         # FICO factor computations
    write*ORCH.js     # Orchestration functions coordinating action sequences
    *RiskParameters   # Conservative and aggressive risk profiles
    simulatePlan.js   # Entry point for full simulation run
```

---

## Tech Stack
- **JavaScript / Node.js** — core engine logic
- **SQL** — scoring logic and data structuring
- **Firebase** — cloud functions, NoSQL database, real-time triggers
- **FlutterFlow** - frontend interface for AppStore/GooglePlay UI components
