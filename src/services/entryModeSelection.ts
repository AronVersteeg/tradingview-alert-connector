const automaticModes = ['green', 'purple', 'atr', 'atr-purple', 'atr-purple-plus'] as const;
export type AutomaticEntryMode = typeof automaticModes[number];
export type EntryMode = AutomaticEntryMode | 'manual' | 'off';
export type EntryModeSelection = EntryMode | `${AutomaticEntryMode},${string}`;

export function parseEntryModes(value: string): { mode: EntryModeSelection; modes: AutomaticEntryMode[]; valid: boolean } {
  const names = value.trim().toLowerCase().split(',').map(name => name.trim() === 'purple-trail' ? 'purple' : name.trim());
  if (names.length === 1 && (names[0] === 'manual' || names[0] === 'off')) {
    return { mode: names[0], modes: [], valid: true };
  }
  const modes = [...new Set(names)] as AutomaticEntryMode[];
  if (modes.some(mode => !automaticModes.includes(mode)) || modes.includes('atr-purple-plus') && !modes.includes('atr-purple')) {
    return { mode: 'off', modes: [], valid: false };
  }
  return { mode: modes.join(',') as EntryModeSelection, modes, valid: true };
}
