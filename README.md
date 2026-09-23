# kafka-viz

A simple website that runs on your own computer and lets you look at and
manage Kafka topics without typing long terminal commands.

**What is Kafka, in one sentence?** It's a system that apps use to send
messages to each other in "topics" (think of a topic as a named mailbox).
This tool lets you see what's in those mailboxes, send test messages, and
clean them up — all by clicking buttons instead of running CLI commands.

**How does it work under the hood?** It doesn't reinvent anything — every
button in this UI just runs one of the official Kafka command-line scripts
for you (the same ones under `bin/` in a Kafka install) and shows you the
result nicely formatted. Nothing new to install on your Kafka side, no
separate database, no extra moving parts.

## What you need before you start

1. **Node.js version 18 or newer**, installed on the same computer you'll
   open the tool from — usually your own laptop, not some faraway server
   (unless that server can also reach your Kafka brokers over the network).
   Check what you have with:
   ```bash
   node -v
   ```
2. **A Kafka installation on your computer** — just a folder containing
   Kafka's `bin/` scripts. If you already have one, skip to
   [Install and run kafka-viz](#install-and-run-kafka-viz). If not, see
   [Getting Kafka onto your computer](#getting-kafka-onto-your-computer) below.
3. **Network access** from your computer to whichever Kafka server you want
   to connect to. For a Kafka you install locally that's just `localhost`,
   which always works. For a shared QA/test/prod Kafka, make sure you're on
   the right VPN or network first.

## Getting Kafka onto your computer

You have two ways to do this. If you're not sure which to pick, use Option A
— it's one command and does everything for you.

### Option A — the easy way (one script does it all)

This repo includes a script that downloads Kafka, checks it's not corrupted,
unpacks it, and points this tool at it automatically.

```bash
npm run setup-kafka                    # downloads and unpacks Kafka into your home folder
npm run setup-kafka -- --start         # ...and also starts it running, ready to use
```

That's it — if you used `--start`, Kafka is now running on your computer and
you can jump to [Install and run kafka-viz](#install-and-run-kafka-viz).

A few extra options if you need them (you usually won't):
- `--dir <folder>` — install somewhere other than your home folder
- `--force` — redo the download even if it's already installed
- `--no-update-config` — don't let the script touch this tool's settings file
- `--help` — show all options

Run the script again any time with `--start` to start Kafka back up; it
won't re-download anything unless you pass `--force`.

### Option B — doing it by hand

Use this if you'd rather understand and control each step yourself.

1. **Make sure Java is installed** — Kafka is a Java program, so it needs a
   Java runtime (version 11 or newer) to run. Check with:
   ```bash
   java -version
   ```
   If that fails, install one — on a Mac, `brew install openjdk@17` works well.

2. **Download Kafka** — go to
   [kafka.apache.org/downloads](https://kafka.apache.org/downloads), find the
   **Binary downloads** section (not "Source download" — that's for people
   building Kafka themselves), and download any version, e.g.
   `kafka_2.13-3.2.1.tgz`.

3. **Unpack it** somewhere permanent, like straight into your home folder:
   ```bash
   tar -xzf ~/Downloads/kafka_2.13-3.2.1.tgz -C ~
   ```
   This creates a folder like `~/kafka_2.13-3.2.1`. Remember this path — it's
   what you'll tell kafka-viz about in a later step.

4. **Start Kafka.** Older versions (like 3.2.1) need a helper program called
   ZooKeeper running first; newer versions don't (see the note below if
   you're using one of those). Open two terminal windows and leave both
   running:
   ```bash
   # Window 1 — start this first, then leave it running
   bin/zookeeper-server-start.sh config/zookeeper.properties

   # Window 2 — start this once Window 1 says it's ready
   bin/kafka-server-start.sh config/server.properties
   ```
   Run these from inside the folder you unpacked in step 3.

5. **Check it worked:**
   ```bash
   bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```
   No error means Kafka is up and reachable (an empty list back is fine —
   it just means there are no topics yet).

To shut Kafka down later, stop the broker window first (Ctrl+C), then the
ZooKeeper window.

<details>
<summary>Using a newer Kafka that doesn't need ZooKeeper (KRaft mode)</summary>

Kafka 3.3 and newer can skip ZooKeeper entirely. From inside the unpacked
folder, run:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

Just the one window to start and stop.
</details>

## Install and run kafka-viz

```bash
cd kafka-viz-tool
npm install
npm start
```

Then open **http://localhost:4545** in your browser.

The first time it runs, it creates a settings file (`config.json`) next to
`server.js` with sensible defaults: it expects Kafka at
`~/kafka_2.13-3.2.1`, and one saved connection called `Local` pointing at
`localhost:9092`.

If your Kafka lives somewhere else, or you want to add more Kafka servers to
switch between (like a shared QA or test environment), click the **gear
icon** in the top right:
- Change the Kafka folder path if yours isn't in the default location
- Add more named connections, e.g.
  - `QA2` → `kafka-01.qa2-sg.cld:9092,kafka-02.qa2-sg.cld:9092`
  - `Prod` → `...`
- Remove connections you no longer need

Everything you set here is saved to `config.json` on your own machine only
— it's excluded from version control (`.gitignore`), so your real server
addresses never get committed or shared. If you're setting this up fresh
from a clone and want to see what that file should look like,
check [`config.example.json`](config.example.json).

## How to use it

There are two pages, switchable from the tabs at the top: **Topics &
messages** and **Publish**. A dropdown next to them lets you switch which
Kafka connection you're pointed at (the ones you configured above).

The sun/moon icon toggles between light and dark mode. It follows your
system's setting to start, and remembers your choice after that.

### Topics & messages page

This is where you browse what's already in Kafka.

- **The list on the left** shows every topic on the selected connection.
  Type in the box above it to filter the list down (matches as you type).
  The circular arrow button reloads the list, and the drag handle on the
  right edge lets you resize the panel if names are getting cut off.
- **Click a topic** to load its most recent messages (the last 50, newest
  first, combined from every partition). The text under the topic name
  tells you how far back in time that batch of messages goes.
- **"Load older messages"**, at the bottom of the message list, fetches the
  next 50 messages further back in time and adds them to what's already
  showing — click it repeatedly (or just once) to page back as far as you
  need. It disappears once you've reached the very first message on the
  topic.
- **Search and filters** above the message list let you narrow down what
  you're looking at — free-text search, a specific partition, a key, or a
  from/to date range (also pick these from the calendar rather than typing
  them). These only search through the messages already loaded on your
  screen, not the whole topic, so results appear instantly.
- **Clicking a message** shows its full payload on the right. Above it, a
  search box highlights any matching text within that payload (handy for
  finding a specific field on a long message), and the copy icon copies the
  whole payload to your clipboard exactly as shown.
- **View details** opens a popup showing the topic's partitions, replica
  info, message counts, and which consumer groups are reading from it and
  how far behind they are.
- **Publish message** takes you straight to the Publish page with this
  topic's name already filled in and its partitions ready to pick from — a
  shortcut so you don't have to retype the topic name.
- **The "+" button** next to the filter box creates a brand-new topic,
  where you choose the number of partitions and the replication factor
  yourself. This is the only way to control the partition count — if you
  instead just publish to a topic name that doesn't exist yet, Kafka may
  auto-create it (if the server allows that) with its own default number of
  partitions, which you can't override from here.

**The two red buttons — use with care:**
- **Purge messages** empties every message out of the topic, but keeps the
  topic itself (so anything that publishes to it can keep working).
- **Delete topic** removes the topic completely.

Both ask you to type the topic's exact name into a popup before doing
anything, as a safety check against clicking the wrong one, and both show a
"please wait" screen that blocks all other actions until the operation
finishes. Kafka doesn't support deleting just one partition and keeping the
rest — it's all-or-nothing (delete) or empty-but-keep (purge).

Topics Kafka creates and manages for itself — their names always start with
a double underscore, like `__consumer_offsets` — show an **internal** tag
in the topic list, and both red buttons stay disabled for them. These
topics keep Kafka itself running, so purging or deleting one isn't a normal
cleanup action; this is blocked both in the button (so you can't click it)
and again on the server (so it's blocked even if something calls the API
directly).

### Publish page

This is where you send a test message into a topic.

1. Pick which Kafka connection to send to, top right.
2. Type (or arrive with pre-filled) a **topic name**.
3. Optionally add a **key** — most apps use this to group related messages
   together — and/or pick a **partition** to send it to specifically. The
   partition list is filled in automatically from the real topic once you
   move to the next field; if the topic doesn't exist yet, you can type a
   partition number by hand instead.
4. Type your message as **JSON** in the big text box. It's checked for
   valid JSON as you type, and again on the server before sending, so you
   can't accidentally send something broken.
5. Click **Publish**.

One technical note if you use both a key and a partition together: Kafka
normally uses the key to decide which partition a message goes to, so if you
also explicitly pick a partition, the key still "wins" and the message may
not land where you picked — the tool will warn you when this happens. If you
pick a partition *without* giving your own key, the tool works around this
by quietly generating an internal key that Kafka's own rules will route to
exactly the partition you chose (you'll see that generated key in the
result, just so nothing is hidden).

## Why loading messages can feel a bit slow

For each topic, this tool starts a small program per partition to go read
its messages, then combines and sorts everything for you. Each of those
programs takes a second or two just to start up (that's normal for these
Kafka tools, not something wrong with this app), so a topic with many
partitions takes proportionally longer to load than one with just a few.
If you want it snappier and don't mind seeing fewer messages at once, you
can lower the default of 50 messages in `server.js` (search for `limit=50`).

**"Load older messages"** is cheap per click — it picks up exactly where
the last page of messages left off (per partition), rather than
re-fetching or re-scanning anything you've already loaded.

## What this tool doesn't do

- It doesn't manage consumer groups for you, and has no support for schema
  registries or Avro-formatted messages.
- It doesn't handle secure clusters out of the box (no built-in
  username/password or SSL certificate setup) — it relies on whatever your
  Kafka CLI scripts already do by default. If your Kafka needs that kind of
  security, you'd need to add a `client.properties` file and adjust
  `server.js` to pass it along.
- Searching and filtering messages only looks at what's currently loaded on
  your screen — it doesn't search the entire topic's full history.
- Picking a specific partition when publishing is a best-effort trick (see
  above) — it only works if you don't also supply your own key.
