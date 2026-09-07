import { ManagedPositionCoordinator } from '../src/services/managedPositionCoordinator';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

describe('shared scanner and position-management state', () => {
  test('management progresses during research and the scanner cannot overwrite its stop', async () => {
    let disk = { stop: 100, scanned: false };
    const coordinator = new ManagedPositionCoordinator(() => ({ ...disk }), (s) => { disk = { ...s }; });
    const research = deferred();
    const scanner = coordinator.withState(async (state) => {
      await research.promise;
      state.scanned = true;
    });
    await coordinator.exclusive(() => coordinator.withState(async (state) => { state.stop = 110; }));
    expect(disk.stop).toBe(110);
    research.resolve();
    await scanner;
    expect(disk).toEqual({ stop: 110, scanned: true });
  });

  test('serializes entries and management and releases the lock after an error', async () => {
    const coordinator = new ManagedPositionCoordinator(() => ({}), () => {});
    const entry = deferred();
    const first = coordinator.exclusive(async () => { await entry.promise; throw new Error('entry failed'); });
    const rejected = expect(first).rejects.toThrow('entry failed');
    const manage = jest.fn(async () => {});
    const second = coordinator.exclusive(manage);
    await Promise.resolve();
    expect(manage).not.toHaveBeenCalled();
    entry.resolve();
    await rejected;
    await second;
    expect(manage).toHaveBeenCalledTimes(1);
  });

  test('persists safety changes even if the scanner fails', async () => {
    let disk = { stop: 100 };
    const coordinator = new ManagedPositionCoordinator(() => ({ ...disk }), (s) => { disk = { ...s }; });
    await expect(coordinator.withState(async (state) => {
      state.stop = 110;
      throw new Error('SMTP failed');
    })).rejects.toThrow('SMTP failed');
    expect(disk.stop).toBe(110);
  });

  test('reloads persisted state after all users finish, including after a write failure', async () => {
    const read = jest.fn(() => ({ stop: 100 }));
    const write = jest.fn().mockImplementationOnce(() => { throw new Error('disk'); });
    const coordinator = new ManagedPositionCoordinator(read, write);
    await expect(coordinator.withState(async () => {})).rejects.toThrow('disk');
    await coordinator.withState(async () => {});
    expect(read).toHaveBeenCalledTimes(2);
  });
});
