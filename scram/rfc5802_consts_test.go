// Copyright (c) 2010 IETF Trust and the persons identified as the
// document authors. All rights reserved. Redistribution and use in source
// and binary forms, with or without modification, are permitted provided
// that the following conditions are met:
//
//   - Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//   - Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in
//     the documentation and/or other materials provided with the
//     distribution.
//   - Neither the name of Internet Society, IETF or IETF Trust, nor the
//     names of specific contributors, may be used to endorse or promote
//     products derived from this software without specific prior
//     written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
// FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
// COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
// OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
// AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
// THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package scram_test

// Following values are extracted from section 5 of RFC 5802,
// available at https://datatracker.ietf.org/doc/html/rfc5802#section-5,
// therefore, the rfc5802_consts_test.go file is licensed under
// the Revised BSD License (included at the top of this file).
//
// Other files are licensed under MIT license as usual in the pq module.
var (
	rfc5802User       = "user"
	rfc5802Pass       = "pencil"
	rfc5802Nonce      = []byte("fyko+d2lbbFgONRv9qkxdawL")
	rfc5802ClientMsgs = []string{
		"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
		"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
	}
	rfc5802ServerMsgs = [][]byte{
		[]byte("r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"),
		[]byte("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="),
	}
)
