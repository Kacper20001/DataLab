class PipelineMeta(type):
    """
    Metaklasa do automatycznego rejestrowania i wykonywania kroków pipeline'u.
    """
    def __new__(cls, name, bases, dct):
        steps = []
        for key, value in dct.items():
            if hasattr(value, "_is_step"):
                steps.append(key)
        dct["_steps"] = steps
        return super().__new__(cls, name, bases, dct)