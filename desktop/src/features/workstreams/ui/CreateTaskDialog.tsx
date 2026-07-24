import * as React from "react";

import { cn } from "@/shared/lib/cn";
import { Button } from "@/shared/ui/button";
import { ChooserDialogContent } from "@/shared/ui/chooser-dialog-content";
import { Dialog } from "@/shared/ui/dialog";
import { Input } from "@/shared/ui/input";

import {
  TASK_STATUSES,
  TASK_STATUS_LABELS,
  type TaskStatus,
} from "../lib/vocab";

const FIELD_SHELL_CLASS =
  "rounded-xl border border-input bg-muted/40 transition-colors hover:border-muted-foreground/40 focus-within:border-muted-foreground/50";
const FIELD_CONTROL_CLASS =
  "border-0 bg-transparent shadow-none outline-none ring-0 placeholder:text-muted-foreground/55 focus-visible:ring-0";

export type CreateTaskInput = {
  name: string;
  status: TaskStatus;
  due: string | null;
};

type CreateTaskDialogProps = {
  isCreating: boolean;
  onCreate: (input: CreateTaskInput) => Promise<void>;
  onOpenChange: (open: boolean) => void;
  open: boolean;
};

export function CreateTaskDialog({
  isCreating,
  onCreate,
  onOpenChange,
  open,
}: CreateTaskDialogProps) {
  const [name, setName] = React.useState("");
  const [status, setStatus] = React.useState<TaskStatus>("todo");
  const [due, setDue] = React.useState("");
  const [errorMessage, setErrorMessage] = React.useState<string | null>(null);
  const nameInputRef = React.useRef<HTMLInputElement>(null);
  const submitInFlightRef = React.useRef(false);

  React.useEffect(() => {
    if (!open) return;
    setName("");
    setStatus("todo");
    setDue("");
    setErrorMessage(null);
    const timerId = globalThis.setTimeout(
      () => nameInputRef.current?.focus(),
      50,
    );
    return () => globalThis.clearTimeout(timerId);
  }, [open]);

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (isCreating || submitInFlightRef.current) return;
    const trimmedName = name.trim();
    if (!trimmedName) return;

    submitInFlightRef.current = true;
    setErrorMessage(null);
    try {
      await onCreate({
        name: trimmedName,
        status,
        due: due.trim().length > 0 ? due.trim() : null,
      });
      onOpenChange(false);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Failed to create task.",
      );
    } finally {
      submitInFlightRef.current = false;
    }
  }

  return (
    <Dialog
      onOpenChange={(nextOpen) => {
        if (!nextOpen && isCreating) return;
        onOpenChange(nextOpen);
      }}
      open={open}
    >
      <ChooserDialogContent
        className="max-w-md"
        contentClassName="pt-3"
        data-testid="create-task-dialog"
        description="Tasks are last-write-wins heads; every status move is also recorded as history."
        footer={
          <div className="flex w-full justify-end">
            <Button
              data-testid="create-task-submit"
              disabled={isCreating || name.trim().length === 0}
              form="create-task-form"
              type="submit"
            >
              {isCreating ? "Creating…" : "Create task"}
            </Button>
          </div>
        }
        footerClassName="border-t-0 pt-0"
        headerClassName="pb-2"
        title="New task"
      >
        <form
          className="space-y-5"
          id="create-task-form"
          onSubmit={(event) => void handleSubmit(event)}
        >
          <div className="space-y-1.5">
            <label
              className="text-sm font-medium text-foreground"
              htmlFor="create-task-name"
            >
              Title
            </label>
            <div
              className={cn(
                "flex min-h-11 items-center px-3",
                FIELD_SHELL_CLASS,
              )}
            >
              <Input
                className={cn("h-8 px-0", FIELD_CONTROL_CLASS)}
                data-testid="create-task-name"
                disabled={isCreating}
                id="create-task-name"
                maxLength={256}
                onChange={(event) => {
                  setName(event.target.value);
                  setErrorMessage(null);
                }}
                placeholder="Calibrate the thermocouple probe"
                ref={nameInputRef}
                value={name}
              />
            </div>
          </div>

          <fieldset className="space-y-1.5">
            <legend className="text-sm font-medium text-foreground">
              Start in
            </legend>
            <div
              className="flex flex-wrap gap-1.5"
              data-testid="create-task-status-picker"
            >
              {TASK_STATUSES.map((candidate) => (
                <Button
                  aria-pressed={status === candidate}
                  className="h-8 rounded-full px-3 text-xs"
                  data-testid={`create-task-status-${candidate}`}
                  disabled={isCreating}
                  key={candidate}
                  onClick={() => setStatus(candidate)}
                  size="sm"
                  type="button"
                  variant={status === candidate ? "default" : "outline"}
                >
                  {TASK_STATUS_LABELS[candidate]}
                </Button>
              ))}
            </div>
          </fieldset>

          <div className="space-y-1.5">
            <label
              className="text-sm font-medium text-foreground"
              htmlFor="create-task-due"
            >
              Due date <span className="text-muted-foreground">(optional)</span>
            </label>
            <div
              className={cn(
                "flex min-h-11 items-center px-3",
                FIELD_SHELL_CLASS,
              )}
            >
              <Input
                className={cn("h-8 px-0", FIELD_CONTROL_CLASS)}
                data-testid="create-task-due"
                disabled={isCreating}
                id="create-task-due"
                onChange={(event) => setDue(event.target.value)}
                placeholder="YYYY-MM-DD"
                type="date"
                value={due}
              />
            </div>
          </div>

          {errorMessage ? (
            <p
              className="text-sm text-destructive"
              data-testid="create-task-error"
            >
              {errorMessage}
            </p>
          ) : null}
        </form>
      </ChooserDialogContent>
    </Dialog>
  );
}
