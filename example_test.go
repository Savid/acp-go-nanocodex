package nanocodexacp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	nanocodexacp "github.com/savid/acp-go-nanocodex"
)

func ExampleNewNanocodexOptions() {
	options := nanocodexacp.NewNanocodexOptions(
		nanocodexacp.WithNanocodexModel("gpt-6.1-sol"),
		nanocodexacp.WithNanocodexThinking("medium"),
	)

	fmt.Println(options.Model)
	fmt.Println(options.Thinking)
	// Output:
	// gpt-6.1-sol
	// medium
}

func ExampleServe_initialize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientReader, clientWriter := io.Pipe()
	agentReader, agentWriter := io.Pipe()
	defer clientReader.Close()
	defer clientWriter.Close()
	defer agentReader.Close()
	defer agentWriter.Close()

	done := make(chan error, 1)
	go func() {
		defer agentWriter.Close()
		done <- nanocodexacp.Serve(ctx, clientReader, agentWriter)
	}()
	_, _ = fmt.Fprintln(clientWriter,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	line, _ := bufio.NewReader(agentReader).ReadString('\n')
	cancel()
	_ = clientWriter.Close()
	<-done

	var response struct {
		Result struct {
			AuthMethods       []any `json:"authMethods"`
			AgentCapabilities struct {
				LoadSession        bool `json:"loadSession"`
				PromptCapabilities struct {
					Image bool `json:"image"`
				} `json:"promptCapabilities"`
			} `json:"agentCapabilities"`
		} `json:"result"`
	}
	_ = json.Unmarshal([]byte(line), &response)
	fmt.Println(len(response.Result.AuthMethods))
	fmt.Println(response.Result.AgentCapabilities.LoadSession)
	fmt.Println(response.Result.AgentCapabilities.PromptCapabilities.Image)
	// Output:
	// 0
	// true
	// true
}
