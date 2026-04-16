# DATS 6450 Midterm 2 — Apache Spark on EC2

## Before you start

1. **Start your Spark cluster** on your EC2 dev machine:
   ```bash
   cd ~/6450-spring-2026
   ./project/setup-spark-cluster.sh <YOUR_LAPTOP_IP>
   ```
   Wait until the script reports the master URL (`spark://<IP>:7077`).

2. **Set your master IP** as an environment variable for convenience:
   ```bash
   export MASTER_PRIVATE_IP=<master private IP printed by setup script>
   ```

3. **Run the health check** — do this first, before opening midterm.py:
   ```bash
   uv run python health_check.py spark://$MASTER_PRIVATE_IP:7077
   ```
   If it passes, you will see `Health check PASSED`. If it fails, raise your hand immediately — do not lose time debugging silently.

## Running the midterm

Edit `midterm.py` to replace all `# TODO` blocks with your code. Then run:

```bash
uv run python midterm.py spark://$MASTER_PRIVATE_IP:7077 | tee output.log
```

The `tee` command saves stdout to `output.log` while still printing to the screen. You must commit `output.log` as part of your submission.

## Submitting

When you are finished (or when time is called):

```bash
git add midterm.py RESPONSES.md output.log
git commit -m "final-submission"
git push
```

**The `final-submission` commit message is required for grading.** Do not push additional commits after this.

## Files in this repo

| File | Description |
|---|---|
| `midterm.py` | **Edit this** — your PySpark solutions |
| `RESPONSES.md` | **Edit this** — written answers for Tasks 2d, 3e, 4e |
| `health_check.py` | Pre-flight cluster check — do not edit |
| `output.log` | Generated when you run midterm.py — commit this |

## Honor pledge

By submitting this midterm you affirm that the work is your own, that you have not used AI assistants (ChatGPT, Claude, Copilot, etc.), and that you have not collaborated with other students. Violations are subject to the GWU Academic Integrity Policy.
