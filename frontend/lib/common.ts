/**
 * Explicitly marks a promise as something we won't await
 * @param _promise
 */
export function spawn(_promise: Promise<any>) {} // eslint-disable-line

/**
 * Explicitly mark that a cast is safe.
 * e.g. `safeCast(x as string[])`.
 */
export function safeCast<T>(x: T): T {
  return x;
}

/**
 * Marks that a cast should be checked at runtime.
 * Usually this is at some system boundary, e.g. a message received over the network.
 */
export function uncheckedCast<T>(x: any): T {
  return x;
}

/**
 * Asserts that a branch is never taken.
 * Useful for exhaustiveness checking.
 * @param _x
 */
export function assertNever(_x: never): never {
  throw new Error("unexpected branch taken");
}
