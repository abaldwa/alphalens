
This workspace will run the user's session-start hook script on folder open.

- Hook path: `${HOME}/.context-system/hooks/session-start.sh`
- VS Code task: [Run context session-start hook](./tasks.json)

If the hook doesn't run, ensure the script is executable:

```
chmod +x ~/.context-system/hooks/session-start.sh
```

To make the task run unconditionally on workspace open:

- The task is configured with `runOn: folderOpen` and will re-evaluate on each open.
- VS Code may require workspace trust or explicit permission for automatic tasks — approve the prompt if shown.
- You can allow automatic tasks in this workspace by the workspace setting `task.allowAutomaticTasks: true` (already set in `.vscode/settings.json`).

Verification log:

- The task now appends a timestamped verification line to `[.vscode/context_hook_ran.log](.vscode/context_hook_ran.log)` after running the hook.
- You can inspect recent entries with:

```
tail -n 50 .vscode/context_hook_ran.log
```

