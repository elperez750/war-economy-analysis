def human_readable(num):
    num = float(num)  # make sure it’s numeric
    if num >= 1_000_000_000_000:   # Trillions
        return f"{num/1_000_000_000_000:.1f} T"
    elif num >= 1_000_000_000:     # Billions
        return f"{num/1_000_000_000:.1f} B"
    elif num >= 1_000_000:         # Millions
        return f"{num/1_000_000:.1f} M"
    elif num >= 1_000:             # Thousands
        return f"{num/1_000:.0f} K"
    else:
        return str(num)
