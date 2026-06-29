#!/usr/bin/env bash
# Seed the NBA definitions cache in nba-redis (hand-seeded for the POC; the command
# center owns this later):
#   nba:action:{id}  -> the action def (rules + the facts each rule uses)
#   nba:actions      -> index of action ids
#   nba:rulefacts    -> the UNION of every fact key any rule references; the snapshot
#                       builder only snapshots facts in this set (lean snapshots).
set -e
R() { podman exec -i ais-nba-redis redis-cli "$@"; }

# Action 1 — nudge operators who logged in but haven't tried chat.
echo '{"actionId":"try_chat_nudge","name":"Try Chat Nudge","ttlSeconds":86400,"channels":["email"],"eligibility":[{"fact":"operator.activity.loggedIn","op":"eq","value":true},{"fact":"operator.activity.usedChat","op":"eq","value":false}],"completion":[{"fact":"operator.activity.usedChat","op":"eq","value":true}]}' \
  | R -x set nba:action:try_chat_nudge >/dev/null

# Action 2 — profile-completion email (respects DNC).
echo '{"actionId":"profile_complete_email","name":"Complete Your Profile","ttlSeconds":172800,"channels":["email"],"eligibility":[{"fact":"operator.activity.loggedIn","op":"eq","value":true},{"fact":"operator.profile.isDNC","op":"eq","value":false}],"completion":[{"fact":"operator.profile.complete","op":"eq","value":true}]}' \
  | R -x set nba:action:profile_complete_email >/dev/null

R del nba:actions >/dev/null
R sadd nba:actions try_chat_nudge profile_complete_email >/dev/null

# Derived fact-dependency set (union of all rule facts across actions).
R del nba:rulefacts >/dev/null
R sadd nba:rulefacts \
  operator.activity.loggedIn \
  operator.activity.usedChat \
  operator.profile.isDNC \
  operator.profile.complete >/dev/null

echo "actions:   $(R smembers nba:actions | tr '\n' ' ')"
echo "rulefacts: $(R smembers nba:rulefacts | tr '\n' ' ')"
