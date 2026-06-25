.code
ContextSwitch PROC
    ; RCX = 'from' pointer (ptr to rsp), RDX = 'to' pointer (ptr to rsp)

    ; 1. Save Callee-Saved GPRs
    push rbx
    push rbp
    push rdi
    push rsi
    push r12
    push r13
    push r14
    push r15

    ; 2. Save Non-Volatile XMM Registers (6 through 15)
    ; Each XMM register is 16 bytes. Use movdqu (unaligned): after 8 pushes RSP is
    ; 8 mod 16, not 16-aligned, so movdqa/movaps would #GP here.
    sub rsp, 160 ; Allocate 160 bytes (10 * 16)
    movdqu [rsp + 0], xmm6
    movdqu [rsp + 16], xmm7
    movdqu [rsp + 32], xmm8
    movdqu [rsp + 48], xmm9
    movdqu [rsp + 64], xmm10
    movdqu [rsp + 80], xmm11
    movdqu [rsp + 96], xmm12
    movdqu [rsp + 112], xmm13
    movdqu [rsp + 128], xmm14
    movdqu [rsp + 144], xmm15

    ; 3. Swap Stack Pointers
    mov [rcx], rsp    ; Save old RSP
    mov rsp, [rdx]    ; Load new RSP

    ; 4. Restore Non-Volatile XMM Registers
    movdqu xmm6, [rsp + 0]
    movdqu xmm7, [rsp + 16]
    movdqu xmm8, [rsp + 32]
    movdqu xmm9, [rsp + 48]
    movdqu xmm10, [rsp + 64]
    movdqu xmm11, [rsp + 80]
    movdqu xmm12, [rsp + 96]
    movdqu xmm13, [rsp + 112]
    movdqu xmm14, [rsp + 128]
    movdqu xmm15, [rsp + 144]
    add rsp, 160

    ; 5. Restore Callee-Saved GPRs
    pop r15
    pop r14
    pop r13
    pop r12
    pop rsi
    pop rdi
    pop rbp
    pop rbx

    ret
ContextSwitch ENDP
END