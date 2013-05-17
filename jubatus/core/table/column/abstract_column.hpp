// Jubatus: Online machine learning framework for distributed environment
// Copyright (C) 2012,2013 Preferred Infrastructure and Nippon Telegraph and Telephone Corporation.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License version 2.1 as published by the Free Software Foundation.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

#ifndef JUBATUS_CORE_TABLE_ABSTRACT_COLUMN_HPP_
#define JUBATUS_CORE_TABLE_ABSTRACT_COLUMN_HPP_
#include <vector>
#include <msgpack.hpp>

#include "column_type.hpp"
#include "bit_vector.hpp"

namespace jubatus {
namespace core {
namespace table {

class column_table;
class column_holder;

namespace detail {
class base_column {
public:
  virtual ~base_column() throw() {}
  virtual void dump() const = 0;
  virtual uint64_t size() const = 0;
  virtual std::string type() const = 0;
  virtual void pack_with_index(
      uint64_t index,
      msgpack::packer<msgpack::sbuffer>& pk) const = 0;
private:
  friend class column_holder;
  friend class abstract_column;
  virtual void clear() = 0;
  virtual void remove(uint64_t index) = 0;
};

template <typename T>
class typed_column : public base_column {
public:
  typedef T type_name;
  typed_column() {}
  virtual ~typed_column() throw() { }

  template <typename U>
  void push_back(const U& v) {
    throw type_unmatch_exception(
        "invalid type push_backed: " + pfi::lang::get_typename<U>() +
        " for " + pfi::lang::get_typename<T>());
  }
  void push_back(const T& v) {
    data_.push_back(v);
  }

  template <typename U>
  void insert(uint64_t index, const U& v) {
    throw type_unmatch_exception(
        "invalid type inserted: " + pfi::lang::get_typename<U>() +
        " for " + pfi::lang::get_typename<T>());
  }

  void insert(uint64_t index, const T& v) {
    if (data_.size() <= index) {
      throw array_range_exception(
          "index " + pfi::lang::lexical_cast<std::string>(index) +
          " is over length from " +
          pfi::lang::lexical_cast<std::string>(data_.size()));
    }
    data_.insert(data_.begin() + index, v);
  }

  template <typename U>
  void update(uint64_t index, const U& v) {
    throw type_unmatch_exception(
        "invalid type updated: " + pfi::lang::get_typename<U>() +
        " for " + pfi::lang::get_typename<T>());
  }

  void update(uint64_t index, const T& v) {
    if (data_.size() <= index) {
      throw array_range_exception(
          "index " + pfi::lang::lexical_cast<std::string>(index) +
          " is over length from " +
          pfi::lang::lexical_cast<std::string>(data_.size()));
    }
    data_[index] = v;
  }

  void pack_with_index(
      uint64_t index,
      msgpack::packer<msgpack::sbuffer>& pk) const {
    pk.pack(data_[index]);
  }

  void remove(uint64_t index) {
    if (data_.size() <= index) {
      throw array_range_exception(
          "index " + pfi::lang::lexical_cast<std::string>(index) +
          " is over length from " +
          pfi::lang::lexical_cast<std::string>(data_.size()));
    }
    std::swap(data_[index],data_.back());
    data_.pop_back();
  }

  std::string type() const {
    return pfi::lang::get_typename<T>();
  }

  uint64_t size() const {
    return data_.size();
  }

  void clear() {
    data_.clear();
  }

  T& operator[](uint64_t index) {
    return data_[index];
  }
  const T& operator[](uint64_t index) const {
    return data_[index];
  }

  void dump() const {
    std::cout << "[column (" << pfi::lang::get_typename<T>() << ")"
              << " size:" << data_.size() << " {";
    for (size_t i = 0; i < data_.size(); ++i) {
      std::cout << data_[i];
      std::cout << " ";
    }
    std::cout << "} ]";
  }

private:
  std::vector<T> data_;
  friend class pfi::data::serialization::access;
  template <class Ar>
  void serialize(Ar& ar) {
    ar & MEMBER(data_);
  }
};

template <typename T>
class const_typed_column : public typed_column<T> {
  typedef typed_column<T> typed_column_t;
  using typed_column_t::update;
  using typed_column_t::push_back;
  using typed_column_t::insert;
  using typed_column_t::remove;
 public:
  using typed_column_t::type;
  using typed_column_t::dump;
  using typed_column_t::size;
  using typed_column_t::operator[];
};

template <>
class typed_column<bit_vector> : public detail::base_column {
public:
  void update(uint64_t index, const bit_vector& bv) {}
  void push_back(bit_vector& bv) {}
  void insert(uint64_t index, const bit_vector& bv) {}
  void remove(uint64_t index) {}
  bit_vector& operator[](uint64_t index) {
    static bit_vector bv(12);
    return bv;
  }
  void dump() const {}
  uint64_t size() const {
    return 10;
  }
  void clear() {}
  void pack_with_index(
      uint64_t index,
      msgpack::packer<msgpack::sbuffer>& pk) const {
  }
  std::string type() const {
    return "bit_vector_column";
  }
};

template <>
class const_typed_column<bit_vector> : public typed_column<bit_vector> {
  typedef typed_column<bit_vector> typed_column_t;
  using typed_column_t::update;
  using typed_column_t::push_back;
  using typed_column_t::insert;
  using typed_column_t::remove;
 public:
  using typed_column_t::type;
  using typed_column_t::dump;
  using typed_column_t::size;
  using typed_column_t::operator[];
};


}  // namespace detail

typedef detail::typed_column<int8_t> int8_column;
typedef detail::typed_column<int16_t> int16_column;
typedef detail::typed_column<int32_t> int32_column;
typedef detail::typed_column<int64_t> int64_column;
typedef detail::typed_column<uint8_t> uint8_column;
typedef detail::typed_column<uint16_t> uint16_column;
typedef detail::typed_column<uint32_t> uint32_column;
typedef detail::typed_column<uint64_t> uint64_column;
typedef detail::typed_column<std::string> string_column;
typedef detail::typed_column<float> float_column;
typedef detail::typed_column<double> double_column;
typedef detail::typed_column<bit_vector> bit_vector_column;

typedef detail::const_typed_column<int8_t> const_int8_column;
typedef detail::const_typed_column<int16_t> const_int16_column;
typedef detail::const_typed_column<int32_t> const_int32_column;
typedef detail::const_typed_column<int64_t> const_int64_column;
typedef detail::const_typed_column<uint8_t> const_uint8_column;
typedef detail::const_typed_column<uint16_t> const_uint16_column;
typedef detail::const_typed_column<uint32_t> const_uint32_column;
typedef detail::const_typed_column<uint64_t> const_uint64_column;
typedef detail::const_typed_column<std::string> const_string_column;
typedef detail::const_typed_column<float> const_float_column;
typedef detail::const_typed_column<double> const_double_column;
typedef detail::const_typed_column<bit_vector> const_bit_vector_column;

namespace detail {
class column_holder {
public:
  column_holder() :column_(NULL) {}
  explicit column_holder(base_column* b) : column_(b) {}
  void set(base_column* b) {
    release();
    column_ = b;
  }
  void release() {
    if (column_) {
      delete column_;
    }
  }
  base_column* operator->() {
    return column_;
  }
  const base_column* operator->() const {
    return column_;
  }
  base_column& operator*() {
    return *column_;
  }
  const base_column& operator*() const {
    return *column_;
  }

  template <typename T>
  T& operator[](uint64_t index) {
    return column_[index];
  }
  template <typename T>
  void push_back(const T& v) {
    dynamic_cast<typed_column<T>*>(column_)->push_back(v);
  }
  template <typename T>
  void insert(uint64_t index, const T& v) {
    dynamic_cast<typed_column<T>*>(column_)->insert(index, v);
  }
  template <typename T>
  void update(uint64_t index, const T& v) {
    dynamic_cast<typed_column<T>*>(column_)->update(index, v);
  }

  void pack_with_index(
    uint64_t index,
    msgpack::packer<msgpack::sbuffer>& pkr) const {
    column_->pack_with_index(index, pkr);
  }

  template <typename T>
  typed_column<T>& as() {
    return *dynamic_cast<typed_column<T>*>(column_);
  }
  template <typename T>
  const_typed_column<T>& as() const {
    return *dynamic_cast<const_typed_column<T>*>(column_);
  }

  void swap(column_holder& rhs) {
    base_column* tmp = rhs.column_;
    rhs.column_ = column_;
    column_ = tmp;
  }
  ~column_holder() {
    release();
  }
private:
  friend class pfi::data::serialization::access;
  template <class Ar>
  void serialize(Ar& ar) {
    ar & MEMBER(*column_);
  }
  base_column* column_;
};

class abstract_column {
public:
  abstract_column() {}
  explicit abstract_column(const column_type& type) {
    if (type.is(column_type::int8_type)) {
      column_.set(new int8_column());
    } else if (type.is(column_type::int16_type)) {
      column_.set(new int16_column());
    } else if (type.is(column_type::int32_type)) {
      column_.set(new int32_column());
    } else if (type.is(column_type::int64_type)) {
      column_.set(new int64_column());
    } else if (type.is(column_type::uint8_type)) {
      column_.set(new uint8_column());
    } else if (type.is(column_type::uint16_type)) {
      column_.set(new uint16_column());
    } else if (type.is(column_type::uint32_type)) {
      column_.set(new uint32_column());
    } else if (type.is(column_type::uint64_type)) {
      column_.set(new uint64_column());
    } else if (type.is(column_type::string_type)) {
      column_.set(new string_column());
    } else if (type.is(column_type::float_type)) {
      column_.set(new float_column());
    } else if (type.is(column_type::double_type)) {
      column_.set(new double_column());
    } else if (type.is(column_type::double_type)) {
      column_.set(new double_column());
    }
  }

  template <typename T>
  T& get_column() {
    return column_.as<T::type_name>();
  }
  template <typename T>
  T& get_column() const {
    return column_.as<T::type_name>();
  }

  int8_column& as_int8_column() { return column_.as<int8_t>(); }
  int16_column& as_int16_column() { return column_.as<int16_t>(); }
  int32_column& as_int32_column() { return column_.as<int32_t>(); }
  int64_column& as_int64_column() { return column_.as<int64_t>(); }
  uint8_column& as_uint8_column() { return column_.as<uint8_t>(); }
  uint16_column& as_uint16_column() { return column_.as<uint16_t>(); }
  uint32_column& as_uint32_column() { return column_.as<uint32_t>(); }
  uint64_column& as_uint64_column() { return column_.as<uint64_t>(); }
  string_column& as_string_column() { return column_.as<std::string>(); }
  float_column& as_float_column() { return column_.as<float>(); }
  double_column& as_double_column() { return column_.as<double>(); }
  bit_vector_column& as_bit_vector_column() { return column_.as<bit_vector>(); }

  const_int8_column& as_int8_column() const { return column_.as<int8_t>(); }
  const_int16_column& as_int16_column() const { return column_.as<int16_t>(); }
  const_int32_column& as_int32_column() const { return column_.as<int32_t>(); }
  const_int64_column& as_int64_column() const { return column_.as<int64_t>(); }
  const_uint8_column& as_uint8_column() const { return column_.as<uint8_t>(); }
  const_uint16_column& as_uint16_column() const { return column_.as<uint16_t>(); }
  const_uint32_column& as_uint32_column() const { return column_.as<uint32_t>(); }
  const_uint64_column& as_uint64_column() const { return column_.as<uint64_t>(); }
  const_string_column& as_string_column() const { return column_.as<std::string>(); }
  const_float_column& as_float_column() const { return column_.as<float>(); }
  const_double_column& as_double_column() const { return column_.as<double>(); }
  const_bit_vector_column& as_bit_vector_column() const { return column_.as<bit_vector>(); }

  template <typename T>
  void push_back(const T& v) {
    column_.push_back(v);
  }
  template <typename T>
  void insert(uint64_t index, const T& v) {
    column_.insert(index, v);
  }
  template <typename T>
  void update(uint64_t index, const T& v) {
    column_.update(index, v);
  }
  void remove(uint64_t index) {
    column_->remove(index);
  }

  void clear() {
    column_->clear();
  }

  uint64_t size() const {
    return column_->size();
  }

  void pack_with_index(
      const uint64_t index,
      msgpack::packer<msgpack::sbuffer>& pk) const {
    column_.pack_with_index(index, pk);
  }

  void dump(std::ostream& os, uint64_t index) const {
  }

  void dump() const {
    column_->dump();
  }

  std::string type() const {
    return column_->type();
  }
 private:
  column_holder column_;

  friend class pfi::data::serialization::access;
  template <class Ar>
  void serialize(Ar& ar) {
    ar & MEMBER(column_);
  }

};

}  // namespace detail
}  // namespace table
}  // namespace core
}  // namespace jubatus

#endif
