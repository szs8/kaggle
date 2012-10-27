library(h5r)

readData <- function(group, block) {
   columns = getH5Dataset(group, sprintf("block%d_items", block))
   data = getH5Dataset(group, sprintf("block%d_values", block))
   l = list()
   for (i in 1:dim(data)[2]) {
      l[[columns[i]]] = data[,i]
   }
   data.frame(l)
}

readIndex <- function(group, level) {
   levels = getH5Dataset(group, sprintf("axis1_level%d", level))
   levels = levels[1:length(levels)]
   inds = getH5Dataset(group, sprintf("axis1_label%d", level))
   inds = inds[1:length(inds)] + 1
   levels[inds]
}

## ToDo Use nlevels and nblocks to iterate over all blocks/levels
## in a generalized version
loadFeatures <- function(filename, groupName) {
  f = H5File(filename)
  group = getH5Group(f, groupName)
  nlevels = getH5Attribute(group, "axis1_nlevels")[1]
  nblocks = getH5Attribute(group, "nblocks")[1]
  df <- readData(group, 0)
  df$member_id <- readIndex(group, 0)
  df$year <- readIndex(group, 1)
  return(df)
}

features <- loadFeatures("feat.h5", "features")
