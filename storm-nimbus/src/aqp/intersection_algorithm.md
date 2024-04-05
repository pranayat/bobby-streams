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
    ## x_sphere > x_max && y_sphere > y_max && z_sphere > z_max // sphere centre going away from top right behind corner
    v = (x_max - min(x1,x2,x3,x4)) * (y_max - min(y1,y2,y3,y4)) * (z_max - min(z1,z2,z3,z4))

    ##

    ##

    ##

    ##

    ##

    ##

    ##