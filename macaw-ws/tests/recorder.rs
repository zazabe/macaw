mod common;

use macaw_core::prelude::*;
use macaw_ws::prelude::*;

use common::{WsTestClient, start_echo_server};

#[tokio::test]
async fn test_recorder_multiple_ws_conns() -> Result<(), anyhow::Error> {
    let temp_file = tempfile::NamedTempFile::new()?;
    let test_file = temp_file.path().to_path_buf();
    let (echo_addr, server_handle) = start_echo_server().await?;
    let echo_url = format!("ws://{}", echo_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy_addr = macaw
        .add_ws_proxy("ws_proxy1", "127.0.0.1:0", &echo_url)
        .await?;

    let proxy_url = format!("ws://{}/", proxy_addr);
    let mut client1 = WsTestClient::connect(&proxy_url).await?;
    let mut client2 = WsTestClient::connect(&proxy_url).await?;

    server_handle.wait_conn_count(2).await;

    server_handle.send_to(0, "msg1 for conn0").await?;
    server_handle.send_to(1, "msg1 for conn1").await?;
    assert_eq!(client1.recv().await?, "msg1 for conn0");
    assert_eq!(client2.recv().await?, "msg1 for conn1");

    client1.send("hello from client1").await?;
    assert_eq!(client1.recv().await?, "echo: hello from client1");

    server_handle.send_to(0, "msg2 for conn0").await?;
    server_handle.send_to(1, "msg2 for conn1").await?;
    assert_eq!(client1.recv().await?, "msg2 for conn0");
    assert_eq!(client2.recv().await?, "msg2 for conn1");

    client2.send("hello from client2").await?;
    assert_eq!(client2.recv().await?, "echo: hello from client2");

    client1.close().await?;
    client2.close().await?;

    server_handle.wait_conn_count(0).await;

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await?;

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file)?.as_str())?;
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.origin"# => "[origin]",
        r#".**.headers["sec-websocket-key"]"# => "[sec-websocket-key]",
        r#".**.headers["sec-websocket-version"]"# => "[sec-websocket-version]",
        r#".**.headers.connection"# => "[connection]",
        r#".**.headers.upgrade"# => "[upgrade]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg1 for conn0"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg1 for conn1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from client1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from client1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg2 for conn1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Text": "msg2 for conn0"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from client2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from client2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 0,
            "event": "Disconnect"
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": 1,
            "event": "Disconnect"
          }
        }
      ]
    }
    "#);
    Ok(())
}

#[tokio::test]
async fn test_recorder_multiple_ws_proxies() -> Result<(), anyhow::Error> {
    let temp_file = tempfile::NamedTempFile::new()?;
    let test_file = temp_file.path().to_path_buf();
    let (echo_addr, server_handle) = start_echo_server().await?;
    let echo_url = format!("ws://{}", echo_addr);

    let mut macaw = Macaw::<Recorder>::recorder();
    let proxy1_addr = macaw
        .add_ws_proxy("ws_proxy1", "127.0.0.1:0", &echo_url)
        .await?;
    let proxy2_addr = macaw
        .add_ws_proxy("ws_proxy2", "127.0.0.1:0", &echo_url)
        .await?;

    // Connect to proxy 1 and send a message
    let proxy1_url = format!("ws://{}/", proxy1_addr);
    let mut client1 = WsTestClient::connect(&proxy1_url).await?;
    client1.send("hello from proxy1").await?;
    assert_eq!(client1.recv().await?, "echo: hello from proxy1");
    client1.close().await?;

    // Connect to proxy 2 and send a message
    let proxy2_url = format!("ws://{}/", proxy2_addr);
    let mut client2 = WsTestClient::connect(&proxy2_url).await?;
    client2.send("hello from proxy2").await?;
    assert_eq!(client2.recv().await?, "echo: hello from proxy2");
    client2.close().await?;

    server_handle.wait_conn_count(0).await;

    macaw.exit_handle().exit();
    macaw.record_when_exit(&test_file).await?;

    let file_content: RecordFile =
        serde_json::from_str(std::fs::read_to_string(test_file)?.as_str())?;
    insta::assert_json_snapshot!(file_content, {
        r#".header.record_id"# => "[record_id]",
        r#".header.record_seed"# => "[record_seed]",
        r#".**.timestamp"# => "[timestamp]",
        r#".**.peer_id"# => "[peer_id]",
        r#".**.headers.host"# => "[host]",
        r#".**.headers.origin"# => "[origin]",
        r#".**.headers["sec-websocket-key"]"# => "[sec-websocket-key]",
        r#".**.headers["sec-websocket-version"]"# => "[sec-websocket-version]",
        r#".**.headers.connection"# => "[connection]",
        r#".**.headers.upgrade"# => "[upgrade]",
    }, @r#"
    {
      "header": {
        "record_id": "[record_id]",
        "record_seed": "[record_seed]",
        "timestamp": "[timestamp]"
      },
      "events": [
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from proxy1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from proxy1"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy1",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": "Disconnect"
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Open": {
                "request": {
                  "method": "GET",
                  "uri": "/",
                  "version": "HTTP/1.1",
                  "headers": {
                    "connection": "[connection]",
                    "host": "[host]",
                    "sec-websocket-key": "[sec-websocket-key]",
                    "sec-websocket-version": "[sec-websocket-version]",
                    "upgrade": "[upgrade]"
                  }
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "hello from proxy2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsUpstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Text": "echo: hello from proxy2"
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": {
              "Message": {
                "message": {
                  "Close": null
                }
              }
            }
          }
        },
        {
          "proxy": "ws_proxy2",
          "timestamp": "[timestamp]",
          "WsDownstream": {
            "peer_id": "[peer_id]",
            "event": "Disconnect"
          }
        }
      ]
    }
    "#);
    Ok(())
}
