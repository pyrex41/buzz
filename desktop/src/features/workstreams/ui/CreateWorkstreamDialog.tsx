import * as React from "react";

import { cn } from "@/shared/lib/cn";
import { Button } from "@/shared/ui/button";
import { ChooserDialogContent } from "@/shared/ui/chooser-dialog-content";
import { Dialog } from "@/shared/ui/dialog";
import { Input } from "@/shared/ui/input";
import { Textarea } from "@/shared/ui/textarea";

import { WS_TYPES, WS_TYPE_LABELS, type WsType } from "../lib/vocab";

const FIELD_SHELL_CLASS =
  "rounded-xl border border-input bg-muted/40 transition-colors hover:border-muted-foreground/40 focus-within:border-muted-foreground/50";
const FIELD_CONTROL_CLASS =
  "border-0 bg-transparent shadow-none outline-none ring-0 placeholder:text-muted-foreground/55 focus-visible:ring-0";

export type CreateWorkstreamInput = {
  name: string;
  wsType: WsType;
  description: string;
};

type CreateWorkstreamDialogProps = {
  isCreating: boolean;
  onCreate: (input: CreateWorkstreamInput) => Promise<void>;
  onOpenChange: (open: boolean) => void;
  open: boolean;
};

/**
 * Create dialog for a 35000 head.
 *
 * The `ws-type` picker is a radio group of buttons rather than a `<select>`:
 * the vocabulary is eight fixed values (Hive §5.2), and type is the single
 * choice that shapes how the whole workstream presents, so it earns the
 * surface area over a collapsed control.
 */
export function CreateWorkstreamDialog({
  isCreating,
  onCreate,
  onOpenChange,
  open,
}: CreateWorkstreamDialogProps) {
  const [name, setName] = React.useState("");
  const [wsType, setWsType] = React.useState<WsType>("general");
  const [description, setDescription] = React.useState("");
  const [errorMessage, setErrorMessage] = React.useState<string | null>(null);
  const nameInputRef = React.useRef<HTMLInputElement>(null);
  const submitInFlightRef = React.useRef(false);

  React.useEffect(() => {
    if (!open) return;
    setName("");
    setWsType("general");
    setDescription("");
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
        wsType,
        description: description.trim(),
      });
      onOpenChange(false);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Failed to create workstream.",
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
        className="max-w-lg"
        contentClassName="pt-3"
        data-testid="create-workstream-dialog"
        description="A workstream groups tasks, artifacts, and decisions for one piece of work — code or otherwise."
        footer={
          <div className="flex w-full justify-end">
            <Button
              data-testid="create-workstream-submit"
              disabled={isCreating || name.trim().length === 0}
              form="create-workstream-form"
              type="submit"
            >
              {isCreating ? "Creating…" : "Create workstream"}
            </Button>
          </div>
        }
        footerClassName="border-t-0 pt-0"
        headerClassName="pb-2"
        title="New workstream"
      >
        <form
          className="space-y-5"
          id="create-workstream-form"
          onSubmit={(event) => void handleSubmit(event)}
        >
          <div className="space-y-1.5">
            <label
              className="text-sm font-medium text-foreground"
              htmlFor="create-workstream-name"
            >
              Name
            </label>
            <div
              className={cn(
                "flex min-h-11 items-center px-3",
                FIELD_SHELL_CLASS,
              )}
            >
              <Input
                className={cn("h-8 px-0", FIELD_CONTROL_CLASS)}
                data-testid="create-workstream-name"
                disabled={isCreating}
                id="create-workstream-name"
                maxLength={256}
                onChange={(event) => {
                  setName(event.target.value);
                  setErrorMessage(null);
                }}
                placeholder="Thermal chamber v2"
                ref={nameInputRef}
                value={name}
              />
            </div>
          </div>

          <fieldset className="space-y-1.5">
            <legend className="text-sm font-medium text-foreground">
              Type
            </legend>
            <div
              className="flex flex-wrap gap-1.5"
              data-testid="create-workstream-type-picker"
            >
              {WS_TYPES.map((candidate) => (
                <Button
                  aria-pressed={wsType === candidate}
                  className="h-8 rounded-full px-3 text-xs"
                  data-testid={`create-workstream-type-${candidate}`}
                  disabled={isCreating}
                  key={candidate}
                  onClick={() => setWsType(candidate)}
                  size="sm"
                  type="button"
                  variant={wsType === candidate ? "default" : "outline"}
                >
                  {WS_TYPE_LABELS[candidate]}
                </Button>
              ))}
            </div>
          </fieldset>

          <div className="space-y-1.5">
            <label
              className="text-sm font-medium text-foreground"
              htmlFor="create-workstream-description"
            >
              Description
            </label>
            <div className={cn("px-3 py-2", FIELD_SHELL_CLASS)}>
              <Textarea
                className={cn("min-h-20 resize-none px-0", FIELD_CONTROL_CLASS)}
                data-testid="create-workstream-description"
                disabled={isCreating}
                id="create-workstream-description"
                onChange={(event) => setDescription(event.target.value)}
                placeholder="What is this work, and when is it done?"
                value={description}
              />
            </div>
          </div>

          {errorMessage ? (
            <p
              className="text-sm text-destructive"
              data-testid="create-workstream-error"
            >
              {errorMessage}
            </p>
          ) : null}
        </form>
      </ChooserDialogContent>
    </Dialog>
  );
}
