"""Shared working-memory diff patch utilities."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MemoryPatchResult:
    """Result of applying a working-memory patch."""

    document: str
    message: str
    applied: bool


def apply_memory_patch(
    document: str,
    patch_text: str,
    max_patch_add_chars: int,
) -> MemoryPatchResult:
    """Apply a simplified line-based diff patch to a memory document."""
    if not isinstance(patch_text, str):
        return MemoryPatchResult(document=document, message="Memory patch ignored: patch must be a string", applied=False)

    lines = [line.rstrip() for line in patch_text.splitlines() if line.strip()]
    if not lines or lines[0] != "@@":
        return MemoryPatchResult(document=document, message="Memory patch ignored: invalid diff header", applied=False)

    removals: list[str] = []
    additions: list[str] = []
    added_chars = 0

    for line in lines[1:]:
        if line.startswith("- "):
            removals.append(line[2:])
            continue
        if line.startswith("+ "):
            added_text = line[2:]
            if not added_text:
                return MemoryPatchResult(document=document, message="Memory patch ignored: empty addition", applied=False)
            additions.append(added_text)
            added_chars += len(added_text)
            continue
        return MemoryPatchResult(document=document, message="Memory patch ignored: invalid diff line", applied=False)

    if added_chars > max_patch_add_chars:
        return MemoryPatchResult(
            document=document,
            message=f"Memory patch ignored: added content exceeds {max_patch_add_chars} characters",
            applied=False,
        )

    updated_lines = document.splitlines()
    for text in removals:
        if text not in updated_lines:
            return MemoryPatchResult(document=document, message="Memory patch ignored: deletion target not found", applied=False)
        updated_lines.remove(text)

    updated_lines.extend(additions)
    updated_document = "\n".join(updated_lines)
    return MemoryPatchResult(
        document=updated_document,
        message=f"Memory patch applied: -{len(removals)}, +{added_chars} chars",
        applied=True,
    )
