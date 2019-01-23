
# Code below was used to generate evenly spaces points
import shapely.geometry
import pyproj


# Set up projections
p_ll = pyproj.Proj(init='epsg:4326')
p_mt = pyproj.Proj(init='epsg:3857') # metric; same as EPSG:900913

# Create corners of rectangle to be transformed to a grid
sw = shapely.geometry.Point((-124.0, 25.0)) # -5, 40
ne = shapely.geometry.Point((-64.9,49.0))  # -4, 41

stepsize = 130000 # 130 km grid step size

# Project corners to target projection
s = pyproj.transform(p_ll, p_mt, ne.x, ne.y) # Transform NW point to 3857
e = pyproj.transform(p_ll, p_mt, sw.x, sw.y) # .. same for SE

# Iterate over 2D area
gridpoints = []
x = s[0]
print(x > e[0])

print(s[1] > e[1])

while x > e[0]:
    y = s[1]
    while y > e[1]:
        p = shapely.geometry.Point(pyproj.transform(p_mt, p_ll, x, y))
        gridpoints.append((p.y, p.x))
        y -= stepsize

    x -= stepsize

in_us = []
import reverse_geocoder as rg
res = rg.search(gridpoints)

us_points = []

slope_nw = (39.216, -126.195)
slope_se = (31.786, -118.420)

ca_slope = (slope_se[0] - slope_nw[0]) / (slope_se[1] - slope_nw[1])

for p, r in zip(gridpoints, res):
	if r['cc'] == 'US':
		p_slope = (p[0] - slope_nw[0]) / (p[1] - slope_nw[1])
		if ca_slope < p_slope:
			us_points.append('{},{}'.format(p[0], p[1]))

print(len(us_points))

us_points_string = '\n'.join(us_points)

#print(us_points_string)

with open('points.csv', 'w') as file:
  file.write(us_points_string)
  file.close()

