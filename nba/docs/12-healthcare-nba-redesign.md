# Healthcare NBA — Redesign Spec (the "promise land")

The NBA replaces the **hand-crafted member-engagement journeys a health plan runs today**. The model learns from
those journeys (imitation → *recreate* the playbook), then **optimizes** for the milestone objectives using the
dispositions it observes. Members are **simulated differently per member** (hidden channel/response affinity), so we can
prove the model starts a new member at baseline and **adapts** as facts about them arrive.

This is a multi-system change: **facts** (snapshot), **milestones + eligibility + completion** (rules-engine /
action-library), **effects** (activation layer), **features** (the model). Spec below; build order at the end.

## 1. Fact model (the member state — what the model trains on)

Facts are real, fire HARD completion, and gate eligibility. Three kinds:

**Digital engagement**
- `respondedToOutreach` (bool) — engaged with any outreach
- `registeredForPortal` (bool) — created a member-portal account
- `loggedIn` (bool) — logged into the portal (recency: `daysSinceLogin`)
- `viewedBenefits` (bool) — viewed plan benefits

**Care / clinical**
- `hraCompleted` (bool) — Health Risk Assessment done
- `pcpSelected` (bool) — primary care provider chosen
- `careTeamEngaged` (bool) — engaged a care manager / care team
- `awvCompleted` (bool) — Annual Wellness Visit done (STARS)
- `medAdherent` (bool) — medication adherence met (STARS — PDC ≥ 0.8)
- `mammogramDone` / `a1cControlled` / `colonoscopyDone` (bool) — STARS gap measures

**Member attributes (personalization features, not goals)**
- `riskScore` (num), `ageBand` (num), `chronicConditions` (e.g. diabetes/hypertension flags), `smsConsent`, `isDNC`

## 2. Action catalog (each action DRIVES one fact; eligibility is FACT-BASED)

| Stage | Action | Drives (hard-complete) | Eligible when |
|---|---|---|---|
| Reach | Plan Welcome | `respondedToOutreach` | new / not responded |
| Reach | Re-engage Lapsed Member | `respondedToOutreach` | lapsed (high `daysSinceLogin`) |
| Digital | Portal Registration | `registeredForPortal` | responded, ¬registered |
| Digital | Portal Login Reminder | `loggedIn` | registered, ¬recently loggedIn |
| Digital | Benefits Education | `viewedBenefits` | registered |
| Assess | Health Risk Assessment | `hraCompleted` | active, ¬hraCompleted |
| Assess | Assessment Reminder | `hraCompleted` | ¬hraCompleted |
| Engage | PCP Selection | `pcpSelected` | hraCompleted, ¬pcpSelected |
| Engage | Care Manager Outreach | `careTeamEngaged` | high `riskScore`, ¬careTeamEngaged |
| Engage | Wellness Education | (engagement) | engaged in care |
| Gap (STARS) | Annual Wellness Visit | `awvCompleted` | pcpSelected, ¬awvCompleted |
| Gap (STARS) | Medication Adherence | `medAdherent` | on meds, ¬medAdherent |
| Gap (STARS) | Mammogram Reminder | `mammogramDone` | due, ¬mammogramDone |
| Gap (STARS) | A1C Test Reminder | `a1cControlled` | diabetic, ¬a1cControlled |
| Gap (STARS) | Colonoscopy Reminder | `colonoscopyDone` | due, ¬colonoscopyDone |

Each action is **multi-channel** (email/sms/push/voice as fits; calls voice-only) → the model picks action AND channel.

## 3. Milestones (sensible, fact-based)

| Milestone | Hard-completion |
|---|---|
| Reached | `respondedToOutreach` |
| Registered | `registeredForPortal` |
| Assessed | `hraCompleted` |
| Engaged in Care | `pcpSelected` ∧ `careTeamEngaged` |
| STARS Compliant | `awvCompleted` ∧ `medAdherent` ∧ (`mammogramDone` ∨ `a1cControlled` ∨ `colonoscopyDone`) |

## 4. Hand-crafted journeys (the playbook the model recreates, then beats)

Members are assigned a journey; it prescribes action+channel per stage; dispositions are random.

| Journey | Reach | Assess | Engage | Gap-close |
|---|---|---|---|---|
| Onboarding | Plan Welcome·email | HRA·email | PCP Selection·push | AWV·email |
| STARS / Gap Closure | Care Gap Alert·sms | Screening Reminder·email | Med Adherence·sms | Mammogram/A1C·push |
| Enrollment | Benefits Intro·email | Portal Registration·email | Benefits Education·email | AWV·email |
| Care Management | Welcome Call·voice | Nurse Onboarding·voice | Care Manager Call·voice | Care Goal Call·voice |
| Re-engagement | Re-engage·email | Assessment Reminder·sms | Login Reminder·push | Med Adherence·email |

## 5. Model

- **Features** = the member facts above (digital + care + attributes) + the action-state dispositions + step.
- **Reward** = milestone value gained (Reached < Registered < Assessed < Engaged < STARS), minus send cost.
- **Train**: reconstruct the hand-crafted journeys → CQL. Pass 1 imitation (recreate the playbook), pass 2 optimize.
- **Per-member affinity**: the simulator gives each member a hidden channel-response profile → the model learns it
  from the dispositions it observes (exploration on the unknown), proving start-at-baseline-then-adapt.

## 6. Build order

1. **Facts**: add the fact keys to the snapshot fact-map + the model feature set (FEATURE_COLS/FEATURE_KEYS).
2. **Milestones**: define the 5 fact-based milestones (definitions → rules-engine).
3. **Catalog**: seed the actions with fact-based inclusion + completion (action-library); effects (nba:sim:effect) set
   the driven fact.
4. **Member init**: source_gen seeds member attributes (riskScore, conditions, consent) + zeroed facts.
5. **Scripted playbook scorer**: drives members down their assigned journey → records real history.
6. **Train**: reconstruct → CQL recreate → optimize → promote → it serves live.
7. **Per-member affinity** in the activation layer → adaptation demo.
