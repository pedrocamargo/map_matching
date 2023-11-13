def reverse_azim(azim):
    return (azim + 180) % 360


@staticmethod
def check_if_inside(heading, polygon_heading, tolerance):
    # If checking the tolerance interval will make the angle bleed the [0,360] interval, we have to fix it

    # In case the angle is too big
    if polygon_heading + tolerance > 360:
        if polygon_heading - tolerance > heading:
            heading += 360

    # In case the angle is too small
    if polygon_heading - tolerance < 0:
        polygon_heading += 360
        if heading < 180:
            heading += 360

    if polygon_heading - tolerance <= heading <= polygon_heading + tolerance:
        return True

    # Several data points do NOT have an heading associated, so we consider the possibility that all the links are valid
    if heading == 0:
        return True

    return False
