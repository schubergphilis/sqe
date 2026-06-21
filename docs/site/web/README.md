# Website content (docs/site/web)

This directory holds the authored copy for the getsqe.com marketing site: the about page, performance page, landing page, roadmap, the DuckDB comparison, and the quickstart cards. It is the single content home, all website copy edits happen here, not in the Astro app.

The getsqe Astro app renders these files via the content sync: prose lives in the `.md` files, and the structured lists and tables live in the `.json` files. The Astro app imports the JSON directly, so the structured copy is zero-dependency to consume (no YAML parser). Keep the wording faithful and follow the repo style guide (no emdash, endash, or unicode arrows; placeholders such as account id `123456789012` stay as placeholders). HTML tags and entities inside the JSON values are intentional and the Astro renderer consumes them, leave them as-is.
