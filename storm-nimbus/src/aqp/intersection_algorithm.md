2D
---
## EDGE APPROACH
  ## only all x coordinates are either max or min - 2 edges - top and bottom
    ## y_sphere > y_max // sphere centre above top edge

    ## y_sphere > y_max // sphere centre below bottom

  ## only all y coordinates are either max or min - 2 edges - right and left
    ## x_sphere > x_max // sphere centre right of right edge

    ## x_sphere > x_min // sphere centre left of left edge

## CORNER APPROACH
  ## none of the coordinates are max or min - 4 corners
    ## x_sphere > x_max && y_sphere > y_max // top-right - sphere centre right of right edge and above top edge

    ## x_sphere > x_max && y_sphere < y_min // bottom-right - sphere centre right of right edge and below bottom edge

    ## x_sphere < x_min && y_sphere < y_min // bottom-left - sphere centre left of left edge and below bottom edge

    ## x_sphere < x_min && y_sphere > y_max // top-left - sphere centre left of left edge and above top edge

3D
---
## FACE APPROACH
  ## only all x and z coordinates are either max or min - 2 faces - top and bottom
    ## y_sphere > y_max // sphere centre above top face
    v = (x_max - x_min) * (z_max - z_min) * (y_max - min(y1,y2,y3,y4))

    ## y_sphere < y_min // sphere centre below top face
    v = (x_max - x_min) * (z_max - z_min) * (max(y1,y2,y3,y4) - y_min)

  ## only all y and z coordinates are either max or min - 2 faces - right and left
    ## x_sphere > x_max // sphere centre to the right of right face
    v = (y_max - y_min) * (z_max - z_min) * (x_max - min(x1,x2,x3,x4))

    ## x_sphere < x_min // sphere centre to the left of left face
    v = (y_max - y_min) * (z_max - z_min) * (max(x1,x2,x3,x4) - x_min)

  ## only all x and y coordinates are either max or min - 2 faces - front and back
    ## z_sphere > z_max // sphere centre is behind back face
    v = (x_max - x_min) * (y_max - y_min) * (z_max - min(z1,z2,z3,z4))

    ## z_sphere < z_min // sphere centre is in front of front face
    v = (x_max - x_min) * (y_max - y_min) * (max(z1,z2,z3,z4) - z_min)    

## EDGE APPROACH
  ## only all x coordinates are either max or min - 4 horizontal edges parallel to x axis
    ## y_sphere > y_max && z_sphere > z_max // sphere centre above top face and behind back face
    v = (x_max - x_min) * (y_max - min(y1,y2,y3,y4)) * (z_max - min(z1,z2,z3,z4))

    ## y_sphere > y_max && z_sphere < z_min // sphere centre above top face and in front of front face
    v = (x_max - x_min) * (y_max - min(y1,y2,y3,y4)) * (max(z1,z2,z3,z4) - z_min)

    ## y_sphere < y_min && z_sphere < z_min // sphere centre below bottom face and in front of front face
    v = (x_max - x_min) * (max(y1,y2,y3,y4) - y_min) * (max(z1,z2,z3,z4) - z_min)

    ## y_sphere < y_min && z_sphere > z_max // sphere centre below bottom face and behind back face
    v = (x_max - x_min) * (max(y1,y2,y3,y4) - y_min) * (z_max - min(z1,z2,z3,z4))

  ## only all y coordinates are either max or min - 4 vertical edges parallel to y axis
    ## x_sphere > x_max && z_sphere > z_max // sphere centre above top face and behind back face
    v = (y_max - y_min) * (x_max - min(x1,x2,x3,x4)) * (z_max - min(z1,z2,z3,z4))

    ##

    ##

    ##

  ## only all z coordinates are either max or min - 4 horizontal edges parallel to z axis
    ##

    ##

    ##

    ##

## CORNER APPROACH
  ## none of the coordinates are max or min - 8 corners
    ## x_sphere > x_max && y_sphere > y_max && z_sphere > z_max // sphere centre right of right face, above top face and behind back face
    v = (x_max - min(x1,x2,x3,x4)) * (y_max - min(y1,y2,y3,y4)) * (z_max - min(z1,z2,z3,z4))

    ##

    ##

    ##

    ##

    ##

    ##

    ##

## Observations
  - For 'n' dimensions, we can have maximum 'n-1' degrees of freedom, ie. we can atmost have coordinates of n-1 dimensions be at max/min
    and the coordinates of the remaining dimensions are free to move between max and min
  - For dimensions whose coordinates are at max/min, volume contribution is simply (dim_max - dim_min) eg. (x_max - x_min)
  - For the remaining coordinates that are free to move between max and min, we can have different intersection approaches based on the position of the sphere
    - if the sphere's coordinate for this dimension > dimension_max, then volume contribution = dimension_max - min(all coordinates in this dimension)
    - if the sphere's coordinate for this dimension < dimension_min, then volume contribution = max(all coordinates in this dimension) - dimension_min

## Algorithm
- Find intersection points of the sphere with the cube
- Classify the intersection as CORNER, EDGE, FACE ... depending on whether 0, 1, 2 ... dimensions have coordinates which are all max/min
- For the coordinates in the remaining dimensions which are not at max/min
  - check of sphere's coordinate in that dimension is > or < these coordinates
  - do this for all combinations of max and min with each of these dimensions
- For dimensions at max/min volumen contribution a (dim_max - dim_min)
- For other dimensions
  - if sphere's coordinate in this dimension is > dim_max, volume contribution = dim_max - min(all coordinates in this dimension)
  - if sphere's coordinate in this dimension is < dim_min, volume contribution = max(all coordinates in this dimension) - dim_min