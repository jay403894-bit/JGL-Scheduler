.code
ContextSwitch PROC
	; RCX = 'from' pointer, RDX = 'to' pointer
	; Save the current context (registers) into the 'from' context
	push rbx    ; Save the caller's RBX onto the stack
	push rbp    ; Save the caller's RBP
	push rdi    ; Save RDI
	push rsi    ; Save RSI
	push r12    ; Save R12
	push r13    ; Save R13
	push r14    ; Save R14
	push r15    ; Save R15

	; save RSP into the 'from' struct
	mov [rcx], rsp ; Save the current RSP into the 'from' struct

	;swap to the new RSP
	mov rsp, [rdx] ; Update RSP to point to the 'to' struct's saved stack

	; Restore the new context (registers) from the 'to' context
	pop r15    ; Restore R15
	pop r14    ; Restore R14
	pop r13    ; Restore R13
	pop r12    ; Restore R12
	pop rsi    ; Restore RSI
	pop rdi    ; Restore RDI
	pop rbp    ; Restore RBP
	pop rbx    ; Restore RBX

	ret
	ContextSwitch ENDP
	END