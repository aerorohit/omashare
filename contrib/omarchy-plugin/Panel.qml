import QtQuick
import QtQuick.Controls
import Quickshell
import Quickshell.Io
import qs.Commons
import qs.Ui

// Bar client for the omashare socket. Not an Iroh node.
// Notifications stay on: this panel does not send {"cmd":"attach","kind":"tui"}.
Panel {
  id: root
  moduleName: "aerorohit.omashare"
  ipcTarget: "aerorohit.omashare"

  readonly property string shareGlyph: String.fromCodePoint(0xF0496)
  readonly property string socketPath: {
    var runtime = String(Quickshell.env("XDG_RUNTIME_DIR") || "")
    return runtime !== "" ? runtime + "/omashare.sock" : "/tmp/omashare.sock"
  }
  readonly property bool agentUp: agentSock.connected
  readonly property color foreground: bar ? bar.foreground : Color.foreground
  readonly property color urgent: bar ? bar.urgent : Color.urgent
  readonly property color dim: Qt.darker(foreground, 1.55)
  readonly property string fontFamily: bar ? bar.fontFamily : Style.font.family
  readonly property color hoverFill: bar ? Style.hoverFillFor(bar.foreground, Color.accent) : "transparent"
  readonly property color selectedFill: bar ? Style.selectedFillFor(bar.foreground, Color.accent) : "transparent"
  readonly property color barIconColor: offers.length > 0 ? urgent : (agentUp ? barForeground : Qt.darker(barForeground, 1.55))
  readonly property string iconTip: {
    if (offers.length > 0) return "Omashare · incoming"
    if (agentUp) return "Omashare"
    if (binaryChecked && !binaryPresent) return "Omashare · not installed"
    return "Omashare · down"
  }
  readonly property string statusLine: {
    if (!agentUp && binaryChecked && !binaryPresent) return "Not installed"
    if (!agentUp) return agentLaunching ? "Starting" : "Down"
    return "Up · " + (alias !== "" ? alias : "this device")
  }
  readonly property string sendLabel: {
    var peer = selectedPeer()
    return peer ? ("Send to " + (peer.alias || "device")) : "Send"
  }

  property string alias: ""
  property string selectedId: ""
  property string notice: ""
  property string activity: ""
  property string pickTarget: ""
  property bool agentLaunching: false
  property bool binaryChecked: false
  property bool binaryPresent: false
  property bool socketWasUp: false
  property int missedProbes: 0
  property bool sendInFlight: false
  property bool pickExitSeen: false
  property bool pickTextSeen: false
  property int pickExitCode: 0
  property string pickText: ""
  property var peers: []
  property var offers: []
  property var trustedIds: []
  property var pendingSend: null
  property var pendingTrust: null

  function selectedPeer() {
    for (var i = 0; i < peers.length; i++) {
      if (peers[i].endpoint_id === selectedId) return peers[i]
    }
    return null
  }

  function isTrusted(id) {
    return trustedIds.indexOf(id) >= 0
  }

  function peerListed(id) {
    for (var i = 0; i < peers.length; i++) {
      if (peers[i].endpoint_id === id) return true
    }
    return false
  }

  function peerText(peer) {
    var name = (peer && peer.alias) ? peer.alias : "Device"
    var kind = peer && peer.device_kind ? (" · " + peer.device_kind) : ""
    var pin = peer && peer.pin_required ? " · pin" : ""
    var trust = peer && isTrusted(peer.endpoint_id) ? " · trusted" : ""
    return name + kind + pin + trust
  }

  function fileLabel(files) {
    if (!files || !files.length) return "file"
    var names = []
    for (var i = 0; i < files.length; i++) names.push(files[i].name || "file")
    return names.join(", ")
  }

  function offerTitle(offer) {
    var who = (offer && offer.from_alias) ? offer.from_alias : "Someone"
    return offer && offer.pin_required ? (who + " · PIN") : who
  }

  function sendLine(obj) {
    if (!agentSock.connected) return false
    agentSock.write(JSON.stringify(obj) + "\n")
    agentSock.flush()
    return true
  }

  function probeSocket() {
    if (probe.running) return
    probe.running = true
  }

  function scheduleRetry() {
    if (agentSock.connected) return
    if (binaryChecked && !binaryPresent) return
    retryTimer.restart()
  }

  function openSocket() {
    if (agentSock.connected) return
    agentSock.connected = true
  }

  function onWhich(code) {
    binaryChecked = true
    binaryPresent = code === 0
    probeSocket()
  }

  function onProbe(code) {
    if (!binaryChecked) return
    if (code === 0) {
      missedProbes = 0
      openSocket()
      return
    }
    if (!binaryPresent) {
      agentLaunching = false
      return
    }
    missedProbes += 1
    if (!agentLaunching || missedProbes >= 4) {
      agentLaunching = true
      missedProbes = 0
      Quickshell.execDetached(["setsid", "--", "omashare"])
    }
    scheduleRetry()
  }

  function onSocketState(up) {
    if (up === socketWasUp) return
    socketWasUp = up
    if (up) {
      onAgentReady()
      return
    }
    offers = []
    sendInFlight = false
    scheduleRetry()
  }

  function onAgentReady() {
    agentLaunching = false
    missedProbes = 0
    sendLine({ cmd: "status" })
    sendLine({ cmd: "nearby" })
    if (pendingTrust && sendLine(pendingTrust)) pendingTrust = null
    sendLine({ cmd: "trust_list" })
    flushPending()
  }

  function setPeers(list) {
    peers = list || []
    if (selectedId === "" && peers.length === 1) selectedId = peers[0].endpoint_id
    if (pendingSend && !sendInFlight && peerListed(pendingSend.to)) flushPending()
  }

  function selectPeer(peer) {
    if (peer && peer.endpoint_id) selectedId = peer.endpoint_id
  }

  function movePeer(dy) {
    if (!peers.length) return
    var i = 0
    for (var n = 0; n < peers.length; n++) {
      if (peers[n].endpoint_id === selectedId) { i = n; break }
    }
    i = Math.max(0, Math.min(peers.length - 1, i + dy))
    selectedId = peers[i].endpoint_id
  }

  function upsertOffer(msg) {
    var next = []
    var replaced = false
    for (var i = 0; i < offers.length; i++) {
      if (offers[i].session_id === msg.session_id) {
        next.push(msg)
        replaced = true
      } else {
        next.push(offers[i])
      }
    }
    if (!replaced) next.push(msg)
    offers = next
  }

  function dropOffer(id) {
    var next = []
    for (var i = 0; i < offers.length; i++) {
      if (offers[i].session_id !== id) next.push(offers[i])
    }
    offers = next
  }

  function toggleTrust(peer) {
    if (!peer || !peer.endpoint_id) return
    var id = peer.endpoint_id
    var turnOn = !isTrusted(id)
    var next = []
    for (var i = 0; i < trustedIds.length; i++) {
      if (trustedIds[i] !== id) next.push(trustedIds[i])
    }
    var cmd = turnOn
      ? { cmd: "trust_add", endpoint_id: id, alias: peer.alias || id }
      : { cmd: "trust_remove", endpoint_id: id }
    if (turnOn) next.push(id)
    trustedIds = next
    notice = ""
    if (sendLine(cmd)) sendLine({ cmd: "trust_list" })
    else {
      pendingTrust = cmd
      probeSocket()
    }
  }

  function respond(sessionId, cmdName) {
    if (!sessionId) return
    notice = ""
    if (!sendLine({ cmd: cmdName, session_id: sessionId })) notice = "Agent is down"
  }

  function beginPick() {
    if (!selectedId) {
      notice = "Select a device"
      return
    }
    if (picker.running) return
    pickTarget = selectedId
    pickExitSeen = false
    pickTextSeen = false
    pickText = ""
    pickExitCode = 0
    notice = ""
    picker.running = true
    if (!agentUp) probeSocket()
  }

  function notePickExit(code) {
    pickExitSeen = true
    pickExitCode = code
    finishPick()
  }

  function notePickText(text) {
    pickTextSeen = true
    pickText = String(text || "")
    finishPick()
  }

  function finishPick() {
    if (!pickExitSeen || !pickTextSeen) return
    var paths = []
    var lines = pickText.split("\n")
    for (var i = 0; i < lines.length; i++) {
      var path = lines[i].trim()
      if (path !== "") paths.push(path)
    }
    if (!paths.length) {
      if (pickExitCode !== 0 && pickExitCode !== 1) notice = "File picker failed"
      return
    }
    pendingSend = { paths: paths, to: pickTarget }
    sendInFlight = false
    if (agentSock.connected) flushPending()
    else probeSocket()
  }

  function flushPending() {
    if (!pendingSend || sendInFlight || !agentSock.connected) return
    sendInFlight = true
    if (!sendLine({ cmd: "send", paths: pendingSend.paths, to: pendingSend.to })) sendInFlight = false
  }

  function handleLine(line) {
    var text = String(line || "").replace(/\r$/, "").trim()
    if (text === "") return
    var msg
    try { msg = JSON.parse(text) } catch (e) { return }
    if (!msg || !msg.type) return
    if (msg.type === "hello" || msg.type === "status") {
      if (msg.alias) alias = msg.alias
    } else if (msg.type === "nearby" || msg.type === "nearby_updated") {
      setPeers(msg.peers || [])
    } else if (msg.type === "trusted") {
      var ids = []
      var devices = msg.devices || []
      for (var i = 0; i < devices.length; i++) {
        if (devices[i].endpoint_id) ids.push(devices[i].endpoint_id)
      }
      trustedIds = ids
    } else if (msg.type === "incoming_offer") {
      upsertOffer(msg)
    } else if (msg.type === "offer_resolved") {
      dropOffer(msg.session_id)
    } else if (msg.type === "transfer_progress") {
      var total = Number(msg.bytes_total) || 0
      var done = Number(msg.bytes_done) || 0
      var pct = total > 0 ? Math.min(100, Math.floor(done * 100 / total)) : 0
      activity = pct + "%" + (msg.path ? (" · " + msg.path) : "")
    } else if (msg.type === "transfer_complete") {
      activity = msg.saved_to ? ("Saved " + msg.saved_to) : "Complete"
      notice = ""
    } else if (msg.type === "transfer_failed") {
      activity = ""
      notice = msg.error || "Transfer failed"
    } else if (msg.type === "ok") {
      if (msg.session_id && sendInFlight) {
        sendInFlight = false
        pendingSend = null
        activity = "Sending"
        notice = ""
      }
    } else if (msg.type === "error") {
      notice = msg.error || "Error"
      if (sendInFlight) {
        sendInFlight = false
        if (String(msg.error || "").indexOf("peer not found") < 0) pendingSend = null
      }
    }
  }

  implicitWidth: button.implicitWidth
  implicitHeight: button.implicitHeight

  Component.onCompleted: whichBin.running = true
  onOpenedChanged: if (opened) {
    if (agentSock.connected) {
      sendLine({ cmd: "nearby" })
      sendLine({ cmd: "trust_list" })
    } else if (binaryChecked) {
      probeSocket()
    }
    Qt.callLater(function() { keyCatcher.forceActiveFocus() })
  }

  Socket {
    id: agentSock
    path: root.socketPath
    parser: SplitParser {
      splitMarker: "\n"
      onRead: function(line) { root.handleLine(line) }
    }
    onConnectionStateChanged: root.onSocketState(connected)
    onError: function(err) { if (!connected) root.scheduleRetry() }
  }

  Process {
    id: whichBin
    command: ["sh", "-c", "command -v omashare"]
    onExited: function(code) { root.onWhich(code) }
  }

  Process {
    id: probe
    command: ["test", "-S", root.socketPath]
    onExited: function(code) { root.onProbe(code) }
  }

  // zenity --file-selection --multiple --separator=\n
  Process {
    id: picker
    command: ["zenity", "--file-selection", "--multiple", "--separator=" + "\n"]
    stdout: StdioCollector {
      waitForEnd: true
      onStreamFinished: root.notePickText(text)
    }
    onExited: function(code) { root.notePickExit(code) }
  }

  Timer {
    id: retryTimer
    interval: 1000
    repeat: false
    onTriggered: root.probeSocket()
  }

  BarIconButton {
    id: button
    anchors.fill: parent
    bar: root.bar
    foreground: root.barIconColor
    useActiveColor: false
    text: root.shareGlyph
    tooltipText: root.iconTip
    onPressed: root.toggle()
  }

  KeyboardPanel {
    id: panel
    anchorItem: button
    owner: root
    bar: root.bar
    open: root.opened
    focusTarget: keyCatcher
    contentWidth: panel.fittedContentWidth(Style.space(360))
    contentHeight: panel.fittedContentHeight(column.implicitHeight, Style.space(520))

    PanelKeyCatcher {
      id: keyCatcher
      anchors.fill: parent
      onMoveRequested: function(dx, dy) { if (dy !== 0) root.movePeer(dy) }
      onActivateRequested: root.beginPick()
      onCloseRequested: root.close()
      onTabRequested: function(direction) { root.switchPanel(direction) }

      Flickable {
        id: flick
        anchors.fill: parent
        contentWidth: width
        contentHeight: column.implicitHeight
        clip: true
        boundsBehavior: Flickable.StopAtBounds
        flickableDirection: Flickable.VerticalFlick
        interactive: contentHeight > height
        ScrollBar.vertical: ScrollBar { policy: ScrollBar.AsNeeded }

        Column {
          id: column
          width: flick.width
          spacing: Style.space(8)

          Text {
            width: parent.width
            text: "Omashare"
            textFormat: Text.PlainText
            color: root.foreground
            font.family: root.fontFamily
            font.pixelSize: Style.font.title
            font.bold: true
          }

          Text {
            width: parent.width
            text: root.statusLine
            textFormat: Text.PlainText
            color: root.dim
            font.family: root.fontFamily
            font.pixelSize: Style.font.caption
            font.bold: true
            elide: Text.ElideRight
          }

          Text {
            visible: root.activity !== ""
            width: parent.width
            text: root.activity
            textFormat: Text.PlainText
            color: root.dim
            font.family: root.fontFamily
            font.pixelSize: Style.font.caption
            elide: Text.ElideMiddle
          }

          Text {
            visible: root.notice !== ""
            width: parent.width
            text: root.notice
            textFormat: Text.PlainText
            wrapMode: Text.WordWrap
            color: root.urgent
            font.family: root.fontFamily
            font.pixelSize: Style.font.caption
          }

          PanelSeparator { foreground: root.foreground }

          PanelSectionHeader {
            text: "NEARBY"
            foreground: root.foreground
            fontFamily: root.fontFamily
          }

          Text {
            visible: root.peers.length === 0
            width: parent.width
            text: root.agentUp ? "No devices nearby" : (root.binaryChecked && !root.binaryPresent ? "Not installed" : "Agent is down")
            textFormat: Text.PlainText
            color: root.dim
            font.family: root.fontFamily
            font.pixelSize: Style.font.body
          }

          Repeater {
            model: root.peers
            delegate: Item {
              id: peerRow
              required property var modelData
              required property int index
              width: column.width
              implicitHeight: Math.max(peerName.implicitHeight, trustSwitch.implicitHeight) + Style.space(8)

              Rectangle {
                anchors.fill: parent
                radius: Style.cornerRadius
                color: root.selectedId === peerRow.modelData.endpoint_id
                  ? root.selectedFill
                  : (peerMouse.containsMouse ? root.hoverFill : "transparent")
              }

              MouseArea {
                id: peerMouse
                anchors.fill: parent
                anchors.rightMargin: trustSwitch.width + Style.space(8)
                hoverEnabled: true
                z: 1
                onClicked: root.selectPeer(peerRow.modelData)
              }

              Text {
                id: peerName
                anchors.left: parent.left
                anchors.right: trustSwitch.left
                anchors.verticalCenter: parent.verticalCenter
                anchors.leftMargin: Style.space(8)
                anchors.rightMargin: Style.space(8)
                text: root.peerText(peerRow.modelData)
                textFormat: Text.PlainText
                color: root.foreground
                font.family: root.fontFamily
                font.pixelSize: Style.font.body
                elide: Text.ElideRight
              }

              ToggleSwitch {
                id: trustSwitch
                z: 2
                anchors.right: parent.right
                anchors.rightMargin: Style.space(4)
                anchors.verticalCenter: parent.verticalCenter
                checked: root.isTrusted(peerRow.modelData.endpoint_id)
                cursorRing: false
                foreground: root.foreground
                accent: Color.accent
                onToggled: root.toggleTrust(peerRow.modelData)
              }
            }
          }

          PanelSeparator { visible: root.offers.length > 0; foreground: root.foreground }

          PanelSectionHeader {
            visible: root.offers.length > 0
            text: "INCOMING"
            foreground: root.foreground
            fontFamily: root.fontFamily
          }

          Repeater {
            model: root.offers
            delegate: Column {
              id: offerRow
              required property var modelData
              required property int index
              width: column.width
              spacing: Style.space(4)

              Text {
                width: parent.width
                text: root.offerTitle(offerRow.modelData)
                textFormat: Text.PlainText
                color: root.foreground
                font.family: root.fontFamily
                font.pixelSize: Style.font.body
                font.bold: true
                elide: Text.ElideRight
              }

              Text {
                width: parent.width
                text: root.fileLabel(offerRow.modelData.files)
                textFormat: Text.PlainText
                color: root.dim
                font.family: root.fontFamily
                font.pixelSize: Style.font.caption
                elide: Text.ElideRight
              }

              Row {
                spacing: Style.space(8)
                Button {
                  text: "Accept"
                  bordered: true
                  foreground: root.foreground
                  fontFamily: root.fontFamily
                  onClicked: root.respond(offerRow.modelData.session_id, "accept")
                }
                Button {
                  text: "Decline"
                  bordered: true
                  foreground: root.urgent
                  fontFamily: root.fontFamily
                  onClicked: root.respond(offerRow.modelData.session_id, "decline")
                }
              }
            }
          }

          PanelSeparator { foreground: root.foreground }

          Button {
            width: parent.width
            text: root.sendLabel
            bordered: true
            foreground: root.foreground
            fontFamily: root.fontFamily
            onClicked: root.beginPick()
          }
        }
      }
    }
  }
}
