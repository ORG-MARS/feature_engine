#include "feature_engine/deps/gtest/include/gtest/gtest.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <cstdlib>
#include <iostream>
#include <string>

using tcp = boost::asio::ip::tcp;       // from <boost/asio/ip/tcp.hpp>
namespace http = boost::beast::http;    // from <boost/beast/http.hpp>

class HttpClientTest : public ::testing::Test {
 public:
  void SetUp() { }
  void TearDown() { }
};

// Performs an HTTP POST and prints the response
TEST_F(HttpClientTest, TestHttpClientPost) {
  try {
    // "Usage: http_client <host> <port> <target>";
    // "Example: http_client www.example.com 80"
    auto const host = "stream.service.163.org";
    auto const port = "80";
    auto const target = "/api/docs/attributes";
    int version = 11;

    // The io_context is required for all I/O
    boost::asio::io_context ioc;

    // These objects perform our I/O
    tcp::resolver resolver{ioc};
    tcp::socket socket{ioc};

    // Look up the domain name
    auto const results = resolver.resolve(host, port);

    // Make the connection on the IP address we get from a lookup
    boost::asio::connect(socket, results.begin(), results.end());

    // Set up an HTTP POST request message
    http::request<http::string_body> req{http::verb::post, target, version};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_type, "application/json");
    req.body() = "{\"docid\":[\"E8EM3HDV0529ALKN\"],\"item\":\"rec\",\"requestId\":\"test_123456\",\"code\":\"all\"}";
    req.prepare_payload();  // call prepare_payload before send request

    // Send the HTTP request to the remote host
    http::write(socket, req);

    // This buffer is used for reading and must be persisted
    boost::beast::flat_buffer buffer;

    // Declare a container to hold the response
    http::response<http::dynamic_body> res;

    // Receive the HTTP response
    http::read(socket, buffer, res);

    // Write the message to standard out
    std::cout << res << std::endl;

    // Gracefully close the socket
    boost::system::error_code ec;
    socket.shutdown(tcp::socket::shutdown_both, ec);

    // not_connected happens sometimes
    // so don't bother reporting it.
    //
    if(ec && ec != boost::system::errc::not_connected)
      throw boost::system::system_error{ec};

  } catch(std::exception const& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    // return EXIT_FAILURE;
  }
  // return EXIT_SUCCESS;
}
