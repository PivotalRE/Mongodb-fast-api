# Setup: Railway, the Analyze button, the Claude app plug-in

## 1. Railway (once)

This folder sits in the `PivotalRE/Mongodb-fast-api` repo, next to the old prospecting API.

**Quickest way (what's set up now):** the repo's root `Dockerfile` builds this agent, so the existing Railway service for the repo runs it, on port 8000 as before. The old MongoDB API is not run for now; its build file is kept as `Dockerfile.mongo-api`. On that service you only add the variables from step 3.

**Separate service instead (later, if the old API comes back):**

1. In the Railway project: **New → GitHub Repo → PivotalRE/Mongodb-fast-api**.
2. In the new service, open **Settings → Source**:
   - **Root Directory:** `property-agent`
   - **Watch Paths:** `property-agent/**`, so changes elsewhere in the repo don't redeploy it

   Railway then reads `property-agent/railway.json` and builds `property-agent/Dockerfile`.
3. Under **Variables**, add:
   - `ANTHROPIC_API_KEY`: the Claude API key
   - `PIVOTAL_SERVICE_TOKEN`: a long random secret for the sheet button
   - `PIVOTAL_MCP_TOKEN`: a different long random secret for the Claude app plug-in. Leave it out to keep the plug-in off.
   - optional: `MAX_RUNNING_JOBS` (default 3)

   To make a secret: `python3 -c "import secrets;print(secrets.token_urlsafe(24))"`
4. Under **Settings → Networking**, choose **Generate Domain**. Opening `https://<domain>/health` should show `{"ok": true}`.

From then on, every push to `main` that changes `property-agent/` redeploys automatically, in about 2 minutes. The service won't start if a required variable is missing.

## 2. Analyze button (once, in the master template)

1. Open the template, then **Extensions → Apps Script**. Paste `Code.gs` and save.
2. Reload the sheet. Use the **Pivotal → Set up connection** menu to enter the Railway domain (with https://) and the `PIVOTAL_SERVICE_TOKEN`.
3. **Pivotal → Create Rules tab** adds a "Rules" tab filled with the default rules. From then on, editing that text changes how the agent decides. No code and no redeploy.
4. Optional button: **Insert → Drawing**, draw "Analyze", place it near B4, then **⋮ → Assign script → `analyzeProperty`**.
5. Copies of the template ("File → Make a copy") keep the script and the Rules tab. The script refuses to write into a file whose name contains "(Template)".

The first run asks for permission to connect to an external service and to edit the spreadsheet. Approve it once.

### Using it

1. Make a working copy of the template.
2. Put the parcel number (or Parcel Viewer link) in **B4**, or just the street address in **B3**.
3. Click **Analyze**. It takes about 1–2 minutes.
4. What happens:
   - Only the agreed input cells are written: B3, B6, B7, B11–B16, B21, B23, B25, B26, B39–B45, B50, B51. B4 becomes the Parcel Viewer link.
   - A cell holding a formula is never overwritten. Every written cell gets a note with the status, the reason and the source link.
   - **check** and **missing** cells turn red. When a later confident run clears one, it goes back to the template yellow.
   - The summary shows the cells written and flagged, which rules were used (Rules tab or built-in), the run time and the cost.

If the address matches more than one parcel, the message lists them. Put the right parcel number in B4 and run again.

## 3. Claude app plug-in (each analyst, once)

Custom connectors need a paid Claude plan. On a Team or Enterprise plan, an owner adds the connector once for everyone.

1. In Claude, open **Settings → Connectors → Add custom connector**.
2. Fill in:
   - **Name:** Pivotal property
   - **URL:** `https://<railway-domain>/mcp?key=<PIVOTAL_MCP_TOKEN>`
3. Turn it on in a chat and ask, for example:
   - "Analyze 1256 111th Ave NE, Bellevue" (the full checked analysis, about 1–2 minutes)
   - "What are the sides of parcel 9331800500, and why is the alley No?"
   - "Which permits are on 3302 S Orcas St?"

The tools for looking things up only read public data. Only `start_analysis` costs anything: about $0.20 a run. Treat the connector URL like a password, because it contains the key.
