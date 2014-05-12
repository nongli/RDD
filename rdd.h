#ifndef RDD_H
#define RDD_H

#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

struct Type {
  enum type {
    INT,
    STRING,
  };
};

class Datum {
 public:
  Type::type type() const { return type_; }
  bool is_null() const { return is_null_; }
  void set_is_null(bool v) { is_null_ = v; }

  bool Equals(const Datum* other) const {
    if (this == other) return true;
    if (this->type() != other->type()) return false;
    if (this->is_null() != other->is_null()) return false;
    if (this->is_null()) return true;
    return EqualsInternal(other);
  }

  virtual std::string ToString() const = 0;

  virtual ~Datum() {}

 protected:
  Datum(const Type::type& t) : type_(t), is_null_(false) {}

  virtual int EqualsInternal(const Datum* other) const = 0;

  Type::type type_;
  bool is_null_;
};

template<typename T, Type::type TYPE>
class BasicDatum : public Datum {
 public:
  BasicDatum(T v) : Datum(TYPE), v_(v) {}

  virtual std::string ToString() const {
    std::stringstream ss;
    ss << v_;
    return ss.str();
  }

  T value() const { return v_; }
  T& value() { return v_; }

 protected:
  virtual int EqualsInternal(const Datum* other) const {
    return value() == reinterpret_cast<const BasicDatum<T, TYPE>*>(other)->value();
  }

 private:
  T v_;
};

typedef BasicDatum<int32_t, Type::INT> Int32Datum;
typedef BasicDatum<std::string, Type::STRING> StringDatum;

std::ostream& operator<<(std::ostream& os, const Type::type& t) {
  switch (t) {
    case Type::STRING:
      os << "STRING";
      break;
    case Type::INT:
      os << "INT";
      break;
    default:
      os << "unknown";
      break;
  }
  return os;
}

struct FieldDesc {
  Type::type type;
  std::string name;
};

class Schema {
 public:
  const FieldDesc* GetFieldByIdx(int idx) const {
    if (idx < 0 || idx >= fields_.size()) return NULL;
    return &fields_[idx];
  }

  const FieldDesc* GetFieldByName(const std::string& name) const {
    std::map<std::string, int>::const_iterator it = fields_by_name_.find(name);
    if (it == fields_by_name_.end()) return NULL;
    return &fields_[it->second];
  }

  int num_fields() const { return fields_.size(); }

  Schema* AddField(const FieldDesc& desc) {
    fields_.push_back(desc);
    fields_by_name_[desc.name] = fields_by_name_.size();
    return this;
  }

  Schema* AddField(Type::type t, const std::string& n) {
    FieldDesc desc;
    desc.type = t;
    desc.name = n;
    return AddField(desc);
  }

  Schema* AppendSchema(const Schema* schema) {
    for (int i = 0; i < schema->fields_.size(); ++i) {
      AddField(schema->fields_[i]);
    }
    return this;
  }

  std::string ToString() const {
    std::stringstream ss;
    for (int i = 0; i < fields_.size(); ++i) {
      if (i != 0) ss << ", ";
      ss << fields_[i].name << " " << fields_[i].type;
    }
    ss << std::endl;
    return ss.str();
  }

 private:
  std::vector<FieldDesc> fields_;
  std::map<std::string, int> fields_by_name_;
};

struct Tuple {
  static Tuple* CreateTuple() { return new Tuple(); }

  Tuple* AddDatum(Datum* d) {
    fields.push_back(d);
    return this;
  }

  std::string ToString(const std::string& delim = ",") const {
    std::stringstream ss;
    for (int i = 0; i < fields.size(); ++i) {
      if (i != 0) ss << delim;
      ss << fields[i]->ToString();
    }
    return ss.str();
  }

  bool Equals(const Tuple* other) const {
    if (this->fields.size() != other->fields.size()) return false;
    for (int i = 0; i < fields.size(); ++i) {
      if (!fields[i]->Equals(other->fields[i])) return false;
    }
    return true;
  }

  void Append(std::shared_ptr<const Tuple> src) {
    for (int i = 0; i < src->fields.size(); ++i) {
      fields.push_back(src->fields[i]);
    }
  }

  std::vector<Datum*> fields;
};

class Rdd {
 public:
  virtual std::shared_ptr<Rdd> aggregate(
      std::function<std::shared_ptr<Tuple> (const Rdd*,
          std::shared_ptr<const Tuple> src, std::shared_ptr<Tuple> dst)> update,
      std::function<std::shared_ptr<Tuple> (const Rdd*,
          std::shared_ptr<const Tuple> src, std::shared_ptr<Tuple> dst)> merge) const = 0;
  virtual void cache() const = 0;
  virtual std::shared_ptr<Rdd> cartesian(const Rdd* other) const = 0;
  virtual void checkpoint() const = 0;
  virtual std::shared_ptr<Rdd> coalesce(int num_partitions, bool shuffle = false) const = 0;
  virtual std::vector<std::shared_ptr<const Tuple> > collect() const = 0;
  virtual int64_t count() const = 0;
  virtual std::vector<std::pair<std::shared_ptr<const Tuple>, int64_t> >
      countByValue() const = 0;
  virtual std::shared_ptr<Rdd> distinct() const = 0;
  virtual std::shared_ptr<Rdd> filter(
      std::function<bool (const Rdd*, const Tuple*)>) const = 0;
  virtual std::shared_ptr<const Tuple> first() const = 0;
  virtual std::shared_ptr<Rdd> flatMap(
      std::function<std::shared_ptr<const Tuple> (const Rdd*, std::shared_ptr<const Tuple>)>) const = 0;
  virtual std::shared_ptr<Rdd> fold() const = 0; // TODO?
  virtual std::shared_ptr<Rdd> sample(
      bool with_replacement, double fraction, int seed = 0) const = 0;
  virtual std::shared_ptr<Rdd> subtract(std::shared_ptr<Rdd>) const = 0;
  virtual std::shared_ptr<Rdd> Union(std::shared_ptr<Rdd>) const = 0;

  virtual ~Rdd() {}

  std::shared_ptr<Schema> schema() const { return schema_; }

 protected:
  Rdd(std::shared_ptr<Schema> schema) : schema_(schema) {}
  std::shared_ptr<Schema> schema_;
};

#endif
