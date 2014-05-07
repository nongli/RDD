#include <stdio.h>
#include <iostream>

#include "local-rdd.h"

using namespace std;

LocalRdd::LocalRdd(const string& name, shared_ptr<Schema> schema)
  : Rdd(schema), name_(name) {
}

shared_ptr<Rdd> LocalRdd::aggregate() const {
  Rdd* rdd = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(rdd);
}

void LocalRdd::cache() const { }

shared_ptr<Rdd> LocalRdd::cartesian(const Rdd* other) const {
  shared_ptr<Schema> new_schema(new Schema);
  new_schema->AppendSchema(schema().get());
  new_schema->AppendSchema(other->schema().get());

  LocalRdd* rdd = new LocalRdd("CartesianProduct", new_schema);
  vector<shared_ptr<const Tuple> > other_data = other->collect();
  for (int i = 0; i < data_.size(); ++i) {
    for (int j = 0; j < other_data.size(); ++j) {
      Tuple* t = Tuple::CreateTuple();
      t->Append(data_[i]);
      t->Append(other_data[j]);
      rdd->Add(t);
    }
  }
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(rdd));
}

void LocalRdd::checkpoint() const { }

shared_ptr<Rdd> LocalRdd::coalesce(int num_partitions, bool shuffle) const {
  Rdd* rdd = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(rdd);
}

vector<shared_ptr<const Tuple> > LocalRdd::collect() const {
  return data_;
}

int64_t LocalRdd::count() const {
  return data_.size();
}

vector<pair<shared_ptr<const Tuple>, int64_t> > LocalRdd::countByValue() const {
  vector<pair<shared_ptr<const Tuple>, int64_t> > results;
  for (int i = 0; i < data_.size(); ++i) {
    bool found = false;
    for (int j = 0; j < results.size(); ++j) {
      if (results[j].first->Equals(data_[i].get())) {
        ++results[j].second;
        found = true;
        break;
      }
    }
    if (!found) results.push_back(make_pair(data_[i], 1));
  }
  return results;
}

shared_ptr<Rdd> LocalRdd::distinct() const {
  LocalRdd* result = new LocalRdd("Distinct", schema_);
  for (int i = 0; i < data_.size(); ++i) {
    bool found = false;
    for (int j = 0; j < result->data_.size(); ++j) {
      if (result->data_[j]->Equals(data_[i].get())) {
        found = true;
        break;
      }
    }
    if (!found) result->Add(data_[i]);
  }
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<Rdd> LocalRdd::filter(function<bool (const Rdd*, const Tuple*)> fn) const {
  LocalRdd* result = new LocalRdd("Filter", schema_);
  for (int i = 0; i < data_.size(); ++i) {
    if (fn(this, data_[i].get())) {
      result->Add(data_[i]);
    }
  }
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<const Tuple> LocalRdd::first() const {
  if (count() == 0) return shared_ptr<const Tuple>();
  return data_[0];
}

shared_ptr<Rdd> LocalRdd::flatMap() const {
  LocalRdd* result = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<Rdd> LocalRdd::fold() const {
  LocalRdd* result = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<Rdd> LocalRdd::sample(bool with_replacement, double fraction, int seed) const {
  LocalRdd* result = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<Rdd> LocalRdd::subtract(shared_ptr<Rdd>) const {
  LocalRdd* result = new LocalRdd("Empty", schema_);
  return shared_ptr<Rdd>(reinterpret_cast<Rdd*>(result));
}

shared_ptr<LocalRdd> LocalRdd::FromFile(const string& file, const string& name) {
  shared_ptr<Schema> schema(new Schema());
  schema->AddField(Type::STRING, "data");
  shared_ptr<LocalRdd> rdd(new LocalRdd(name, schema));
  return rdd;
}

const string& LocalRdd::name() const {
  return name_;
}

const Tuple* LocalRdd::Add(const Tuple* t) {
  return Add(shared_ptr<const Tuple>(t));
}

const Tuple* LocalRdd::Add(shared_ptr<const Tuple> t) {
  data_.push_back(t);
  return t.get();
}

string LocalRdd::PrintToTable() const {
  stringstream ss;
  ss << "Name: " << name() << endl;
  for (int i = 0; i < schema_->num_fields(); ++i) {
    ss << schema_->GetFieldByIdx(i)->name << "\t";
  }
  ss << endl;
  ss << "---------------------------------------------" << endl;
  for (int i = 0; i < data_.size(); ++i) {
    ss << data_[i]->ToString("\t") << endl;
  }
  return ss.str();
}

int main(int argc, char** argv) {
  shared_ptr<Schema> schema(new Schema());
  schema->AddField(Type::INT, "Key")->AddField(Type::STRING, "Data");

  shared_ptr<LocalRdd> rdd(new LocalRdd("RDD", schema));
  rdd->Add(Tuple::CreateTuple()->AddDatum(new Int32Datum(1))->AddDatum(new StringDatum("hello")));
  rdd->Add(Tuple::CreateTuple()->AddDatum(new Int32Datum(20))->AddDatum(new StringDatum("world")));
  rdd->Add(Tuple::CreateTuple()->AddDatum(new Int32Datum(20))->AddDatum(new StringDatum("world")));
  cout << rdd->PrintToTable();
  cout << "First: " << rdd->first()->ToString() << endl;

  vector<pair<shared_ptr<const Tuple>, int64_t> > count_by_value = rdd->countByValue();
  for (int i = 0; i < count_by_value.size(); ++i) {
    cout << count_by_value[i].second << ": " << count_by_value[i].first->ToString() << endl;
  }

  shared_ptr<Rdd> product = rdd->cartesian(rdd.get());
  cout << ((LocalRdd*)product.get())->PrintToTable();

  shared_ptr<Rdd> distinct = product->distinct();
  cout << ((LocalRdd*)distinct.get())->PrintToTable();

  shared_ptr<Rdd> filter = distinct->filter(
    [](const Rdd*, const Tuple* t) {
      return reinterpret_cast<const Int32Datum*>(t->fields[0])->value() == 20;
    } );
  cout << ((LocalRdd*)filter.get())->PrintToTable();

  printf("Done.\n");
  return 0;
}
