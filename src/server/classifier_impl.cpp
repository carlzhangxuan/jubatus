// This file is auto-generated from classifier.idl
// *** DO NOT EDIT ***

#include <map>
#include <string>
#include <vector>
#include <utility>

#include "../framework.hpp"
#include "classifier_server.hpp"
#include "classifier_serv.hpp"

namespace jubatus {
namespace server {

class classifier_impl_ : public classifier<classifier_impl_> {
 public:
  explicit classifier_impl_(const jubatus::framework::server_argv& a):
    classifier<classifier_impl_>(a.timeout),
    p_(new jubatus::framework::server_helper<classifier_serv>(a, false)) {
  }
  std::string get_config(std::string name) {
    JRLOCK__(p_);
    return get_p()->get_config();
  }

  int32_t train(std::string name, std::vector<std::pair<std::string,
       datum> > data) {
    JWLOCK__(p_);
    return get_p()->train(data);
  }

  std::vector<std::vector<estimate_result> > classify(std::string name,
       std::vector<datum> data) {
    JRLOCK__(p_);
    return get_p()->classify(data);
  }
  std::string dump(std::string name, const std::string& format) const {
    JRLOCK__(p_);
    return get_p()->dump(format);
  }

  bool clear(std::string name) {
    JWLOCK__(p_);
    return get_p()->clear();
  }

  bool save(std::string name, std::string id) {
    JWLOCK__(p_);
    return get_p()->save(id);
  }

  bool load(std::string name, std::string id) {
    JWLOCK__(p_);
    return get_p()->load(id);
  }

  std::map<std::string, std::map<std::string, std::string> > get_status(
      std::string name) {
    JRLOCK__(p_);
    return p_->get_status();
  }
  int run() { return p_->start(*this); }
  common::cshared_ptr<classifier_serv> get_p() { return p_->server(); }
  const common::cshared_ptr<classifier_serv> get_p() const { return p_->server(); }

 private:
  common::cshared_ptr<jubatus::framework::server_helper<classifier_serv> > p_;
};

}  // namespace server
}  // namespace jubatus

int main(int argc, char* argv[]) {
  return
    jubatus::framework::run_server<jubatus::server::classifier_impl_>
      (argc, argv, "classifier");
}
