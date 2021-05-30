// Package angora is a root package containing all the necessary files for using angora.
package angora

/*
----------------- Features -------------
1) Supports reconnecting(exponential backoff) to RabbitMQ server, when Server comes up again.
2) PublishConfirm for reliable Publishing.
3) Callback registration for failed publishing, if published message could not be confirmed.
4) Supports Channels Pool.
5) Supports Consumer using grouping to manage all the consumers as a group.
6) Graceful shutdown for all the consumers and publishers.
7) TODO - graceful shutdown of confirm channels (wait for all the confirmations to arrive).

*/
