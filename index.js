const fs = require('fs');
const { copyFile } = require('fs/promises');
const BSON = require('bson');
const YAML = require('js-yaml');

class SaracenDB {
  constructor(filename, collection = 'default') {
    this._filename = filename;
    this._data = {};
    this._coll = collection;
    this._deleted = false;
    if (fs.existsSync(this._filename)) {
      this._data = BSON.deserialize(fs.readFileSync(this._filename));
    }
    if (!this._data[this._coll]) {
      this._data[this._coll] = [];
    }
  }

  get file() {
    return this._filename;
  }

  get colls() {
    return Object.keys(this._data);
  }

  get len() {
    return Object.keys(this._data).length;
  }

  get collLen() {
    return this.data[this._coll].length;
  }
  get all() {
    return this._data;
  }

  find(key, value) {
    return this._data[this._coll].filter((item) => item[key] === value);
  }

  filter(keys, values) {
    const matchingEntries = [];
    for (const entry of this._data[this._coll]) {
      if (keys.every((key, index) => entry[key] === values[index])) {
        matchingEntries.push(entry);
      }
    }
    return matchingEntries;
  }

  get(id) {
    for (const entry of this._data[this._coll]) {
      if (entry['#'] === id) {
        return entry;
      }
    }
    console.log(`No entry found with id: ${id} in collection: ${this._coll}`);
    return null;
  }

  add(entry) {
    if (typeof entry !== 'object' || Array.isArray(entry)) {
      throw new TypeError('Entry must be of type object.');
    } else {
      const ids = this._data[this._coll].map((item) => item['#']);
      const addData = { '#': ids.length ? ids[ids.length - 1] + 1 : 0 };
      Object.assign(addData, entry);
      this._data[this._coll].push(addData);
    }
  }

  edit(key, new_value, id) {
    if (key === '#') {
      throw new Error('Cannot edit the id of an entry.');
    }
    for (const entry of this._data[this._coll]) {
      if (entry['#'] === id) {
        entry[key] = new_value;
        return;
      }
    }
    console.log(`No entry found with id: ${id} in collection: ${this._coll}`);
  }

  editMany(key, new_value, ids) {
    if (key === '#') {
      throw new Error('Cannot edit the id of an entry.');
    }
    for (const entry of this._data[this._coll]) {
      if (ids.includes(entry['#'])) {
        entry[key] = new_value;
      }
    }
  }

  editAll(key, new_value) {
    if (key === '#') {
      throw new Error('Cannot edit the id of an entry.');
    }
    for (const entry of this._data[this._coll]) {
      entry[key] = new_value;
    }
  }

  addColl(collection) {
    if (this._data.hasOwnProperty(collection)) {
      console.log(`Collection already exists for name: ${collection}`);
    } else {
      this.push();
      this._data[collection] = [];
      this._coll = collection;
    }
  }

  useColl(collection) {
    this.push();
    if (collection in this._data) {
      this._coll = collection;
      console.log(`Switched to collection: ${collection}`);
    } else {
      this.addColl(collection);
      this._coll = collection;
      console.log(`Created and switched to collection: ${collection}`);
    }
  }

  delItem(id) {
    for (let i = 0; i < this._data[this._coll].length; i++) {
      const entry = this._data[this._coll][i];
      if (entry['#'] === id) {
        this._data[this._coll].splice(i, 1);
        this._deleted = true;
        console.log(
          `Entry with id: ${id} deleted from collection: ${this._coll}`
        );
        return;
      }
    }
    console.log(`No entry found with id: ${id} in collection: ${this._coll}`);
  }

  delItems(ids) {
    for (const id of ids) {
      let found = false;
      for (let i = 0; i < this._data[this._coll].length; i++) {
        const entry = this._data[this._coll][i];
        if (entry['#'] === id) {
          this._data[this._coll].splice(i, 1);
          this._deleted = true;
          console.log(
            `Entry with id: ${id} deleted from collection: ${this._coll}`
          );
          found = true;
          break;
        }
      }
      if (!found) {
        console.log(
          `No entry found with id: ${id} in collection: ${this._coll}`
        );
      }
    }
  }

  delKey(key, id) {
    if (key === '#') {
      throw new Error('Cannot delete the id of an entry.');
    }
    for (const entry of this._data[this._coll]) {
      if (entry['#'] === id) {
        if (key in entry) {
          delete entry[key];
          this._deleted = true;
          return;
        }
      }
    }
    console.log(`No entry found with id: ${id} in collection: ${this._coll}`);
  }

  delKeys(keys, id) {
    if (keys.includes('#')) {
      throw new Error('Cannot delete the id of an entry.');
    }
    for (const key of keys) {
      for (const entry of this._data[this._coll]) {
        if (entry['#'] === id) {
          if (key in entry) {
            delete entry[key];
            this._deleted = true;
            return;
          }
        }
      }
    }
    console.log(`No entry found with id: ${id} in collection: ${this._coll}`);
  }

  delKeyForAll(key) {
    if (key === '#') {
      throw new Error('Cannot delete the id of an entry.');
    }

    for (const entry of this._data[this._coll]) {
      if (key in entry) {
        delete entry[key];
        this._deleted = true;
      }
    }
  }

  delKeysForAll(keys) {
    if (keys.includes('#')) {
      throw new Error('Cannot delete the id of an entry.');
    }

    for (const key of keys) {
      for (const entry of this._data[this._coll]) {
        if (key in entry) {
          delete entry[key];
          this._deleted = true;
        }
      }
    }
  }

  delColl(collection) {
    if (Object.keys(this._data).length === 1) {
      console.log('Cannot delete the last collection.');
      return;
    }
    if (collection === this._coll) {
      console.log('Cannot delete the collection while using it.');
      return;
    }
    try {
      delete this._data[collection];
      this._deleted = true;
    } catch (error) {
      // Collection doesn't exist, do nothing
    }
  }

  push() {
    fs.writeFileSync(this._filename, BSON.serialize(this._data));
    if (this._deleted) {
      this.compact();
      this._deleted = false;
    }
  }

  compact() {
    const tempFilename = this._filename + '.tmp';
    fs.writeFileSync(tempFilename, BSON.serialize(this._data));
    fs.renameSync(tempFilename, this._filename);
  }

  toJson(coll = null, cust_path = null) {
    let data;
    let file_path = '';
    if (!coll || coll === '') {
      coll = 'db';
      data = this._data;
    } else {
      data = this._data[coll];
    }
    if (!cust_path) {
      file_path = `${coll}.json`;
    } else {
      file_path = cust_path;
    }
    fs.writeFileSync(file_path, JSON.stringify(data, null, 2));
  }

  toYaml(coll = null, cust_path = null) {
    let data;
    let file_path = '';
    if (!coll || coll === '') {
      coll = 'db';
      data = this._data;
    } else {
      data = this._data[coll];
    }
    if (!cust_path) {
      file_path = `${coll}.yaml`;
    } else {
      file_path = cust_path;
    }
    fs.writeFileSync(file_path, YAML.dump(data));
  }

  addJson(path) {
    const file = JSON.parse(fs.readFileSync(path, 'utf8'));
    if (!Array.isArray(file)) {
      throw new TypeError('File must be of type list.');
    }
    for (const entry of file) {
      if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) {
        throw new TypeError('File must be of type list of dicts.');
      }
    }
    for (const entry of file) {
      delete entry['#'];
    }
    const updatedFile = file.map((entry, i) => ({ '#': i, ...entry }));
    this._data[this._coll] = this._data[this._coll].concat(updatedFile);
  }

  addYaml(path) {
    const file = YAML.load(fs.readFileSync(path, 'utf8'));
    if (!Array.isArray(file)) {
      throw new TypeError('File must be of type list.');
    }
    for (const entry of file) {
      if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) {
        throw new TypeError('File must be of type list of dicts.');
      }
    }
    for (const entry of file) {
      delete entry['#'];
    }
    const updatedFile = file.map((entry, i) => ({ '#': i, ...entry }));
    this._data[this._coll] = this._data[this._coll].concat(updatedFile);
  }

  reindex() {
    for (let i = 0; i < this._data[this._coll].length; i++) {
      const entry = this._data[this._coll][i];
      entry['#'] = i;
    }
    this.push();
    this.compact();
  }

  getColl() {
    return this._data[this._coll];
  }

  setColl(coll) {
    if (!Array.isArray(coll)) {
      throw new TypeError('Collection must be of type list.');
    }
    this._data[this._coll] = coll;
    console.log(`Collection ${this._coll} overwritten.`);
  }

  async backup(path = 'backup.db') {
    const sourcePath = this._filename;
    const destinationPath = path;
    await copyFile(sourcePath, destinationPath);
  }
}
module.exports = SaracenDB;
