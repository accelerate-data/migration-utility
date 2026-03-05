export async function setup(): Promise<void> {
  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error(
      "ANTHROPIC_API_KEY is not set. Export it before running integration tests."
    );
  }
}
