from pipeline.meta import PipelineMeta

class BasePipeline(metaclass=PipelineMeta):
    """
    Klasa bazowa każdego pipeline'u. Wykonuje zarejestrowane kroki.
    """
    def run(self):
        for step_name in self._steps:
            step = getattr(self, step_name)
            print(f"\n[Pipeline] Wykonuję krok: {step_name}")
            step()
