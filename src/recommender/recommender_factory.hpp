// Jubatus: Online machine learning framework for distributed environment
// Copyright (C) 2011 Preferred Infrastructure and Nippon Telegraph and Telephone Corporation.
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

#ifndef JUBATUS_RECOMMENDER_RECOMMENDER_FACTORY_HPP_
#define JUBATUS_RECOMMENDER_RECOMMENDER_FACTORY_HPP_

#include <string>
#include <pficommon/text/json.h>

namespace jubatus {

namespace jsonconfig {

class config;
}  // namespace jsonconfig

namespace recommender {

class recommender_base;

class recommender_factory {
 public:
  static recommender_base* create_recommender(
      const std::string& name,
      const jsonconfig::config& param);
};

}  // namespace recommender

}  // namespace jubatus

#endif  // JUBATUS_RECOMMENDER_RECOMMENDER_FACTORY_HPP_
