// is obj an Object (not an array)
const isObject = (obj) => {
  return (typeof obj === 'object' && !Array.isArray(obj))
}

// combine two keys
const combine = (k1, k2) => {
  return k1 ? k1 + '_' + k2 : k2
}

// to obj, add the value to the object and delete the
// original. If there's a prefix, the new key is the
// combination of the prefix and the key e.g
//    address.street.housenumber
const combineAssign = (obj, prefix, key, value) => {
  if (prefix) {
    obj[combine(prefix, key)] = value
    delete obj[key]
  } else {
    obj[key] = value
  }
}

// recursively flatten a JavaScript object
const flatten = (obj, prefix) => {
  // for top level keys
  if (!prefix) {
    prefix = ''
    obj.id = obj._id
    obj.rev = obj._rev
    delete obj._id
    delete obj._rev
  }

  // go through each key
  for (let i in obj) {
    // if we find an object
    if (isObject(obj[i])) {
      // recurse into that object and flatten it
      const newobj = flatten(obj[i], combine(prefix, i))
      Object.assign(obj, newobj)
      delete obj[i]
    } else {
      // calculate value to put in the object
      const v = Array.isArray(obj[i]) ? JSON.stringify(obj[i]) : obj[i]
      combineAssign(obj, prefix, i, v)
    }
  }
  return obj
}

module.exports = {
  flatten: flatten
}
