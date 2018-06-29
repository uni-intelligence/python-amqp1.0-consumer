def import_name(name):
    components = name.split('.')
    mod = __import__(
        '.'.join(components[0:-1])
    )
    for m in components[1:]:
        mod = getattr(mod, m)
    return mod
