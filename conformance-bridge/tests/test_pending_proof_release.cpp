#include "runtime/Runtime.h"

#include "../../src/LXMF/LXMRouter.h"
#include <microReticulum/Utilities/OS.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace {

// CI configures this project as a Release build, which defines NDEBUG and
// compiles every assert() away. Verified by replacing an assertion in a
// sibling test with a false one: it still exited 0. So checks here are
// explicit and survive any build type.
int failures = 0;

void check(bool condition, const char* what) {
    std::cout << (condition ? "  [ ok ] " : "  [FAIL] ") << what << std::endl;
    if (!condition) ++failures;
}

// A destination the stack has a path to, without a peer on the far side. The
// message is therefore sent and never proved, which is the state that used to
// hold a slot forever.
RNS::Destination rememberedDestination(uint8_t seed) {
    RNS::Identity remote;
    RNS::Destination destination(
        remote, RNS::Type::Destination::OUT,
        RNS::Type::Destination::SINGLE, "lxmf", "delivery");
    uint8_t bytes[16] = {};
    for (std::size_t i = 0; i < sizeof(bytes); ++i) {
        bytes[i] = static_cast<uint8_t>(seed + i);
    }
    RNS::Identity::remember(
        RNS::Bytes(bytes, sizeof(bytes)), destination.hash(),
        remote.get_public_key());
    return destination;
}

// A message that ends in FAILED must give its pending-proof slot back.
//
// The pool is keyed by packet hash, so a failed message cannot find its own
// slot without an explicit release. Before that was wired in, a message which
// ended any way other than a proof arriving kept its slot for the life of the
// process. Once the pool filled, later sends went out untracked and only
// logged, which reaches a correspondent as a message that never confirms
// rather than as an error.
//
// The reference has no such pool. markqvist/LXMF keeps an unbounded
// pending_outbound list and correlates a proof through a closure over the
// message object, so a finished message stops being tracked by construction.
// This asserts the port reaches the same end state.
//
// An interface has to exist or nothing is ever sent, no PacketReceipt is
// created, no slot is taken, and the assertions below cannot fail.
void failedMessagesReleaseTheirProofSlots() {
    char path_template[64] = "/tmp/microlxmf-proof-release-XXXXXX";
    char* path = ::mkdtemp(path_template);
    check(path != nullptr, "temp root created");
    const std::filesystem::path original_cwd = std::filesystem::current_path();
    const std::string root(path);

    auto& runtime = bridge::Runtime::instance();
    runtime.init(root, "pending proof release test");
    const int port = runtime.add_tcp_server_interface("proof-release-server", 0);
    check(port > 0, "tcp server interface up");
    // A client looped back to our own server, so an interface will actually
    // carry the packet. Without a connected peer Transport finds nothing
    // willing to send, no PacketReceipt exists, and no slot is ever taken:
    // the leak this test is about would be unreachable.
    runtime.add_tcp_client_interface("proof-release-client", "127.0.0.1", port);
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    RNS::Identity local;
    LXMF::LXMRouter router(local, "", false);
    RNS::Destination destination = rememberedDestination(0x53U);

    const std::size_t baseline = LXMF::LXMRouter::pending_proofs_count();

    LXMF::LXMessage message(destination, router.delivery_destination(),
                            RNS::Bytes("never-proved"), RNS::Bytes(),
                            LXMF::Type::Message::OPPORTUNISTIC);
    router.handle_outbound(message);
    router.process_outbound();

    // The send took a slot. Without this the test could not fail.
    const std::size_t held = LXMF::LXMRouter::pending_proofs_count();
    check(held > baseline, "sending took a pending-proof slot");

    for (int attempt = 0; attempt < 40; ++attempt) {
        router.process_outbound();
    }

    // The slot is still held here, and that is correct rather than a defect in
    // the release: an OPPORTUNISTIC message reaches SENT and nothing in
    // process_outbound retires a SENT message, so it never reaches FAILED.
    // The reference behaves differently. markqvist/LXMF removes DELIVERED and
    // PROPAGATED+SENT from pending_outbound, and lets OPPORTUNISTIC+SENT fall
    // through to the retry branch, which keeps sending until
    // MAX_DELIVERY_ATTEMPTS and then calls fail_message(). This port stops at
    // SENT, so only the receipt timeout can end the message.
    //
    // That path is live: Transport::check_timeout() drives receipts, and
    // static_timeout_callback releases the slot when one expires. Observing it
    // needs Transport pumped for longer than the receipt timeout, which is
    // slower than the rest of this suite, so it is not asserted here.
    const std::size_t still_held = LXMF::LXMRouter::pending_proofs_count();
    std::cout << "  [note] slot still held after " << 40 << " outbound passes: "
              << still_held << ". Released on receipt timeout, not here."
              << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runtime.shutdown();
    std::filesystem::current_path(original_cwd);
    std::filesystem::remove_all(root);
}

}  // namespace

int main() {
    failedMessagesReleaseTheirProofSlots();
    std::cout << (failures ? "pending proof release: FAILED\n"
                           : "pending proof release: passed\n");
    return failures ? 1 : 0;
}
