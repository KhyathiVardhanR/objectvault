#pragma once

// Multithreaded HTTP/1.1 server on Boost.Asio + Boost.Beast. An acceptor
// thread hands each connection to a fixed worker pool; a worker serves the
// connection's requests (keep-alive) until the client closes it.

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <atomic>
#include <functional>
#include <iostream>
#include <string>
#include <thread>

namespace ov {

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;

using Request = http::request<http::string_body>;
using Response = http::response<http::string_body>;
using Handler = std::function<void(const Request&, Response&)>;

class HttpServer {
 public:
  HttpServer(const std::string& host, unsigned short port, size_t workers, uint64_t body_limit, Handler h)
      : acceptor_(io_, tcp::endpoint(asio::ip::make_address(host), port)),
        pool_(workers), body_limit_(body_limit), handler_(std::move(h)) {}

  void run() {
    for (;;) {
      tcp::socket sock(io_);
      boost::system::error_code ec;
      acceptor_.accept(sock, ec);
      if (ec) continue;
      sock.set_option(tcp::no_delay(true));
      asio::post(pool_, [this, s = std::make_shared<tcp::socket>(std::move(sock))]() mutable {
        serve(std::move(*s));
      });
    }
  }

 private:
  void serve(tcp::socket sock) {
    beast::flat_buffer buf;
    boost::system::error_code ec;
    for (;;) {
      http::request_parser<http::string_body> parser;
      parser.body_limit(body_limit_);
      http::read(sock, buf, parser, ec);
      if (ec) break;  // closed, or malformed
      Request req = parser.release();
      Response res{http::status::ok, req.version()};
      res.set(http::field::server, "objectvault");
      res.keep_alive(req.keep_alive());
      try {
        handler_(req, res);
      } catch (const std::exception& e) {
        res = Response{http::status::internal_server_error, req.version()};
        res.set(http::field::content_type, "application/json");
        res.body() = std::string("{\"error\":\"internal\",\"message\":\"") + e.what() + "\"}";
      }
      bool head = req.method() == http::verb::head;
      if (!head) res.prepare_payload();
      http::serializer<false, http::string_body> sr{res};
      sr.split(false);
      if (head) {
        http::write_header(sock, sr, ec);
      } else {
        http::write(sock, sr, ec);
      }
      if (ec || !res.keep_alive()) break;
    }
    sock.shutdown(tcp::socket::shutdown_send, ec);
  }

  asio::io_context io_;
  tcp::acceptor acceptor_;
  asio::thread_pool pool_;
  uint64_t body_limit_;
  Handler handler_;
};

}  // namespace ov
