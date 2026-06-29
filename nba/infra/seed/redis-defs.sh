#!/usr/bin/env sh
# Captured NBA Redis definition state (rulefacts lean-filter + sim params).
# Replayed by up.ps1 via: podman exec -i ais-nba-redis sh < this-file
redis-cli del nba:rulefacts >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.email.daily >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.email.rate >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.mail.daily >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.push.daily >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.push.rate >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.sms.daily >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.sms.rate >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.voice.daily >/dev/null
redis-cli sadd nba:rulefacts nba.throttle.voice.rate >/dev/null
redis-cli sadd nba:rulefacts operator.activity.a1cControlled >/dev/null
redis-cli sadd nba:rulefacts operator.activity.awvCompleted >/dev/null
redis-cli sadd nba:rulefacts operator.activity.careTeamEngaged >/dev/null
redis-cli sadd nba:rulefacts operator.activity.colonoscopyDone >/dev/null
redis-cli sadd nba:rulefacts operator.activity.hraCompleted >/dev/null
redis-cli sadd nba:rulefacts operator.activity.loggedIn >/dev/null
redis-cli sadd nba:rulefacts operator.activity.mammogramDone >/dev/null
redis-cli sadd nba:rulefacts operator.activity.medAdherent >/dev/null
redis-cli sadd nba:rulefacts operator.activity.pcpSelected >/dev/null
redis-cli sadd nba:rulefacts operator.activity.registeredForPortal >/dev/null
redis-cli sadd nba:rulefacts operator.activity.respondedToOutreach >/dev/null
redis-cli sadd nba:rulefacts operator.activity.viewedBenefits >/dev/null
redis-cli sadd nba:rulefacts operator.comms.emailsThisWeek >/dev/null
redis-cli sadd nba:rulefacts operator.comms.totalThisWeek >/dev/null
redis-cli sadd nba:rulefacts operator.profile.diabetic >/dev/null
redis-cli sadd nba:rulefacts operator.profile.isDNC >/dev/null
echo eyJhY3Rpb25fcGxhbl93ZWxjb21lIjogeyJyZXNwb25kZWRUb091dHJlYWNoIjogMS4wfSwgImFjdGlvbl9yZWVuZ2FnZSI6IHsicmVzcG9uZGVkVG9PdXRyZWFjaCI6IDEuMH0sICJhY3Rpb25fcG9ydGFsX3JlZ2lzdHJhdGlvbiI6IHsicmVnaXN0ZXJlZEZvclBvcnRhbCI6IDEuMH0sICJhY3Rpb25fbG9naW5fcmVtaW5kZXIiOiB7ImxvZ2dlZEluIjogMS4wfSwgImFjdGlvbl9iZW5lZml0c19lZHVjYXRpb24iOiB7InZpZXdlZEJlbmVmaXRzIjogMS4wfSwgImFjdGlvbl9ocmEiOiB7ImhyYUNvbXBsZXRlZCI6IDEuMH0sICJhY3Rpb25faHJhX3JlbWluZGVyIjogeyJocmFDb21wbGV0ZWQiOiAxLjB9LCAiYWN0aW9uX3BjcF9zZWxlY3Rpb24iOiB7InBjcFNlbGVjdGVkIjogMS4wfSwgImFjdGlvbl9jYXJlX21hbmFnZXJfb3V0cmVhY2giOiB7ImNhcmVUZWFtRW5nYWdlZCI6IDEuMH0sICJhY3Rpb25fd2VsbG5lc3NfZWR1Y2F0aW9uIjogeyJjYXJlVGVhbUVuZ2FnZWQiOiAxLjB9LCAiYWN0aW9uX2FubnVhbF93ZWxsbmVzc192aXNpdCI6IHsiYXd2Q29tcGxldGVkIjogMS4wfSwgImFjdGlvbl9tZWRfYWRoZXJlbmNlIjogeyJtZWRBZGhlcmVudCI6IDEuMH0sICJhY3Rpb25fbWFtbW9ncmFtIjogeyJtYW1tb2dyYW1Eb25lIjogMS4wfSwgImFjdGlvbl9hMWNfdGVzdCI6IHsiYTFjQ29udHJvbGxlZCI6IDEuMH0sICJhY3Rpb25fY29sb25vc2NvcHkiOiB7ImNvbG9ub3Njb3B5RG9uZSI6IDEuMH19Cg== | base64 -d | redis-cli -x set nba:sim:effect >/dev/null
echo eyJhY3Rpb25fcGxhbl93ZWxjb21lIjogeyJiIjogMC42LCAiZGF5c1NpbmNlTG9naW4iOiAwLjAxfSwgImFjdGlvbl9yZWVuZ2FnZSI6IHsiYiI6IDAuMywgImRheXNTaW5jZUxvZ2luIjogMC4wMn0sICJhY3Rpb25fcG9ydGFsX3JlZ2lzdHJhdGlvbiI6IHsiYiI6IDAuMX0sICJhY3Rpb25fbG9naW5fcmVtaW5kZXIiOiB7ImIiOiAtMC4yfSwgImFjdGlvbl9iZW5lZml0c19lZHVjYXRpb24iOiB7ImIiOiAtMC4zfSwgImFjdGlvbl9ocmEiOiB7ImIiOiAtMC4yfSwgImFjdGlvbl9ocmFfcmVtaW5kZXIiOiB7ImIiOiAtMC40fSwgImFjdGlvbl9wY3Bfc2VsZWN0aW9uIjogeyJiIjogLTAuNH0sICJhY3Rpb25fY2FyZV9tYW5hZ2VyX291dHJlYWNoIjogeyJiIjogLTEuNiwgInJpc2tTY29yZSI6IDAuNH0sICJhY3Rpb25fd2VsbG5lc3NfZWR1Y2F0aW9uIjogeyJiIjogLTEuMn0sICJhY3Rpb25fYW5udWFsX3dlbGxuZXNzX3Zpc2l0IjogeyJiIjogLTAuNn0sICJhY3Rpb25fbWVkX2FkaGVyZW5jZSI6IHsiYiI6IC0wLjh9LCAiYWN0aW9uX21hbW1vZ3JhbSI6IHsiYiI6IC0wLjl9LCAiYWN0aW9uX2ExY190ZXN0IjogeyJiIjogLTAuN30sICJhY3Rpb25fY29sb25vc2NvcHkiOiB7ImIiOiAtMS4wfX0K | base64 -d | redis-cli -x set nba:sim:fstar >/dev/null
echo MC4wMwo= | base64 -d | redis-cli -x set nba:sim:fail_rate >/dev/null
echo MC4wMgo= | base64 -d | redis-cli -x set nba:sim:decline_rate >/dev/null
echo ZGVsaXZlcnkK | base64 -d | redis-cli -x set nba:sim:mode >/dev/null
echo eyJhY3Rpb25fZDc1YmZiOWUiOiAwLjA2LCAiYWN0aW9uX2RlOTY0YTczIjogMC4xMiwgImFjdGlvbl82ZWEzMzZmOCI6IDAuMSwgImFjdGlvbl85YzIyZWEyMiI6IDAuM30K | base64 -d | redis-cli -x set nba:sim:propensity >/dev/null
echo eyJlbWFpbCI6IHsicG9ydGFsTG9naW5zMzBkIjogMC4wMywgInBhZ2VzVmlld2VkMzBkIjogMC4wMTIsICJhZ2UiOiAtMC4wMDR9LCAicHVzaCI6IHsicG9ydGFsTG9naW5zMzBkIjogMC4wNSwgImFnZSI6IC0wLjAwOH0sICJzbXMiOiB7InBvcnRhbExvZ2luczMwZCI6IDAuMDQsICJhZ2UiOiAtMC4wMDR9LCAidm9pY2UiOiB7ImNvbW9yYmlkaXR5Q291bnQiOiAwLjIyLCAic2RvaEJhcnJpZXIiOiAwLjgsICJlclZpc2l0czEybW8iOiAwLjE1LCAiYWdlIjogMC4wMDIsICJwb3J0YWxMb2dpbnMzMGQiOiAtMC4wMjV9LCAibWFpbCI6IHsic2RvaEJhcnJpZXIiOiAwLjYsICJjb21vcmJpZGl0eUNvdW50IjogMC4xMiwgImFnZSI6IDAuMDAzLCAicG9ydGFsTG9naW5zMzBkIjogLTAuMDMsICJwYWdlc1ZpZXdlZDMwZCI6IC0wLjAwOH19Cg== | base64 -d | redis-cli -x set nba:sim:channel_affinity >/dev/null
echo "  seeded: rulefacts=$(redis-cli scard nba:rulefacts)"
