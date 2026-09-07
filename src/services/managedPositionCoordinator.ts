// Scanner and position management share one state object while either is active.
// Only position-changing work is serialized; slow research must not hold this lock.
export class ManagedPositionCoordinator<State> {
  private state: State | undefined;
  private users = 0;
  private positionWork: Promise<void> = Promise.resolve();

  constructor(
    private readonly read: () => State,
    private readonly write: (state: State) => void
  ) {}

  async withState<T>(operation: (state: State) => Promise<T>): Promise<T> {
    if (this.users === 0) this.state = this.read();
    const state = this.state as State;
    this.users += 1;
    try {
      return await operation(state);
    } finally {
      try {
        this.write(state);
      } finally {
        this.users -= 1;
        if (this.users === 0) this.state = undefined;
      }
    }
  }

  async exclusive<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.positionWork;
    let release!: () => void;
    this.positionWork = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }
}
