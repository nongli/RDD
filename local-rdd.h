#ifndef LOCAL_RDD_H
#define LOCAL_RDD_H

#include "rdd.h"

class LocalRdd : public Rdd {
 public:
  virtual std::shared_ptr<Rdd> aggregate() const;
  virtual void cache() const;
  virtual std::shared_ptr<Rdd> cartesian(const Rdd* other) const;
  virtual void checkpoint() const;
  virtual std::shared_ptr<Rdd> coalesce(int num_partitions, bool shuffle = false) const;
  virtual std::vector<std::shared_ptr<const Tuple> > collect() const;
  virtual int64_t count() const;
  virtual std::vector<std::pair<std::shared_ptr<const Tuple>, int64_t> > countByValue() const;
  virtual std::shared_ptr<Rdd> distinct() const;
  virtual std::shared_ptr<Rdd>
      filter(std::function<bool (const Rdd*, const Tuple*)>) const;
  virtual std::shared_ptr<const Tuple> first() const;
  virtual std::shared_ptr<Rdd> flatMap(
      std::function<std::shared_ptr<const Tuple> (const Rdd*, std::shared_ptr<const Tuple>)>) const;
  virtual std::shared_ptr<Rdd> fold() const; // TODO?
  virtual std::shared_ptr<Rdd> sample(
      bool with_replacement, double fraction, int seed) const; // TODO?
  virtual std::shared_ptr<Rdd> subtract(std::shared_ptr<Rdd>) const;

 public:
  LocalRdd(const std::string& name, std::shared_ptr<Schema> schema);
  static std::shared_ptr<LocalRdd> FromFile(
      const std::string& file, const std::string& name = "data");

  const std::string& name() const;
  std::string PrintToTable() const;

  const Tuple* Add(const Tuple* t);
  const Tuple* Add(std::shared_ptr<const Tuple> t);

 private:
  std::string name_;
  std::vector<std::shared_ptr<const Tuple> > data_;
};

#endif
