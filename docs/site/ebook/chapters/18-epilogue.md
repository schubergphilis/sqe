# Epilogue

I started this book with a question from the security team: "Who accessed the customer table last Tuesday?"

Seventeen chapters later, we can answer it. The query ran as Alice. S3 saw Alice. CloudTrail shows Alice. The audit log shows Alice's query, Alice's tables, Alice's execution time. No service account. No shared secret. No ambiguity.

That was the problem. It's solved.

But the real thing I built isn't a query engine. It's a set of opinions about how data infrastructure should work, opinions that happen to compile.

The opinion that authentication is an architectural constraint, not a feature. The opinion that policy enforcement belongs in the query plan, not around it. The opinion that a catalog should be a standard interface, not a vendor's moat. The opinion that distribution should be a conscious decision, not a default. The opinion that an engine you can't configure is an engine you can't trust.

These opinions could have been implemented in Java. They could have been implemented in Go. They ended up in Rust because Rust's type system makes wrong opinions hard to compile. The borrow checker doesn't care about your architecture, but it catches the moment your architecture stops making sense: when a shared reference crosses a thread boundary it shouldn't, when a credential outlives the session it belongs to, when a plan node gets moved after something else borrowed it.

I used an AI coding agent to build most of this in fifteen days. The agent wrote the Rust. I wrote the opinions. The agent is very good at Rust. It has no opinions of its own.

That division of labour is the real discovery. Not that AI can write code. Everyone knows that. The discovery is that the things AI can't do are exactly the things that make a project worth doing. Choosing bearer passthrough over service accounts. Deciding to fork Ballista's model instead of using it as-is. Knowing when to stop distributing and let a single node handle it. Recognising that the timestamp precision bug was in the display formatter, not the data.

The puzzle is still the point. It has been since I was a kid taking things apart. The tools have changed, from a screwdriver to a compiler to an AI agent that can hold twelve crates in its head simultaneously. But the drive is the same: understand how it works, make it better, move on to the next problem.

The next problem is already forming. The engine queries tables. But the questions people actually ask aren't about tables. They're about relationships, patterns, meanings. "Which customers are at risk?" is not a SQL query. It's a question that requires context no table schema captures. The semantic layer (property graphs, vector search, agent interfaces) is where the engine goes next.

But that's a different book.

This one started with a Trino cluster that worked. Mostly. It ends with an engine that answers the security team's question. Fully.

If you've read this far, you now know how to build a query engine. You also know when not to.

Build what matters. Leave the rest.

---

*Amsterdam, 2026*
