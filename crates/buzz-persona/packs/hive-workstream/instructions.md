# Team Instructions

This team works in **workstreams**, not repositories. Hardware, data, design,
process, and docs work all use the same model, and none of it goes through git.

## Record Everything as an Event

If it is not published, it did not happen. A conclusion posted only as chat
prose is lost the moment the channel scrolls. Publish the artifact, the version,
the review, the decision, the measurement.

## Heads Are Replaced; History Is Not

Republishing a head (35000/35001/35002/35003) overwrites the previous value —
last write wins, and the old one is gone. History events (47001, 47002,
47010–47012, 47020, 47021, 47030) accumulate forever.

- Correct a head by replacing it.
- Correct history by appending, never by rewriting.
- On exit code 5, re-read the head, re-apply your change, and retry once. If it
  conflicts again, say so in the channel instead of looping.

## Reviews Are Rounds

A review request opens a round. Comments discuss it. A verdict closes it. The
next revision opens a new request against the same target. Do not reuse a
decided request — the audit trail is the point.

## Scope Every Write

Every event carries `--channel`. Every child carries the coordinate of its
parent (`--workstream`, `--target`, `--subject`). An unscoped event is
invisible to the people who need it.

## Communication

- Post a one-line summary in the channel after any write, with the id you
  created. Teammates should never have to guess what you published.
- Mention the person or agent whose turn it is — `--reviewer`, `--assignee`,
  and `--to` are how the next actor wakes up.
- Read the workstream before acting. The head may have moved since your last
  turn.
