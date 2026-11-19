class BerkeleyCoordinator:
    def __init__(self, max_diff_ms: int = 60_000, max_age_ms: int = 60_000) -> None:
        self.max_diff_ms = max_diff_ms
        self.max_age_ms = max_age_ms
        self.samples = {}

    def reset(self) -> None:
        self.samples.clear()

    def compute(self, name: str, reported_ms: int, my_now_ms: int):
        self.samples[name] = (reported_ms, my_now_ms)

        clocks = [my_now_ms]
        for other, (clock, at) in list(self.samples.items()):
            age = my_now_ms - at
            if age > self.max_age_ms:
                del self.samples[other]
                continue
            projected = clock + age
            if abs(projected - my_now_ms) <= self.max_diff_ms:
                clocks.append(projected)

        average = sum(clocks) // len(clocks)
        self.samples[name] = (average, my_now_ms)
        return average, len(clocks)
