# Based on https://gist.github.com/jeromer/2005586
import math


def compute_line_bearing(pointA, pointB):
    latA = math.radians(pointA[0])
    latB = math.radians(pointB[0])

    delta_long = math.radians(pointB[1] - pointA[1])

    x = math.sin(delta_long) * math.cos(latB)
    y = math.cos(latA) * math.sin(latB) - (math.sin(latA) * math.cos(latB) * math.cos(delta_long))

    bearing_radians = math.atan2(x, y)
    bearing_degrees = math.degrees(bearing_radians)
    compass_bearing = (bearing_degrees + 360) % 360

    return compass_bearing
