asm!(
            // ---- Where the line ends: the first newline 41..104 bytes on.
            "vmovdqu ymm6, ymmword ptr [rsi + 41]",
            "vmovdqu ymm7, ymmword ptr [rsi + 73]",
            "vpcmpeqb ymm6, ymm6, ymmword ptr [rdi + {c_nl}]",
            "vpcmpeqb ymm7, ymm7, ymmword ptr [rdi + {c_nl}]",
            "vpmovmskb r15d, ymm6",
            "vpmovmskb r14d, ymm7",
            "shl r14, 32",
            "or r15, r14",
            "tzcnt r15, r15",
            "jc 88f",
            // ---- The line's structure.
            "vmovdqu ymm0, ymmword ptr [rsi]",
            "vmovdqu ymm1, ymmword ptr [rsi + 32]",
            "vpcmpeqb ymm2, ymm0, ymmword ptr [rdi + {c_comma}]",
            "vpcmpeqb ymm3, ymm1, ymmword ptr [rdi + {c_comma}]",
            "vpcmpeqb ymm0, ymm0, ymmword ptr [rdi + {c_quote}]",
            "vpcmpeqb ymm1, ymm1, ymmword ptr [rdi + {c_quote}]",
            "vpmovmskb eax, ymm2",
            "vpmovmskb ecx, ymm3",
            "vpmovmskb edx, ymm0",
            "vpmovmskb r8d, ymm1",
            "shl rcx, 32",
            "or rax, rcx",              // commas
            "shl r8, 32",
            "or rdx, r8",               // quotes
            "rorx r8, rax, 1",
            "and r8, rdx",              // closing quotes: a comma follows
            "lea r9, [rdx + rdx]",
            "andn r9, r9, rax",         // commas with no quote before them
            "or r9, r8",                // r9: where each value ends
            "lea r10, [rax + 1]",       // separators: the line's start and the commas
            "lea r11, [r10 + r10]",
            "and r11, rdx",             // opening quotes: a separator precedes
            "rorx r12, rdx, 1",
            "andn r12, r12, r10",       // separators with no quote after them
            "or r12, r11",              // r12: one before where each value starts
            "pext r11, r11, r12",       // by field: opens with a quote
            "pext r8, r8, r9",          // by field: closes with one
            "xor r11, r8",
            "test r11b, 7",
            "jnz 88f",                  // one quote without the other, in the first three fields
            // ---- The two IDs: prefix, then digits that end where the value ends.
            "tzcnt r10, r9",
            "tzcnt r11, r12",
            "blsr r9, r9",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r11 + 1], 0x65",
            "jne 88f",
            "vmovq xmm0, qword ptr [rsi + r10 - 8]",
            "sub r10, r11",             // the event ID's length plus 1
            "tzcnt r13, r9",
            "tzcnt r14, r12",
            "blsr r9, r9",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r14 + 1], 0x75",
            "jne 88f",
            "vpinsrq xmm0, xmm0, qword ptr [rsi + r13 - 8], 1",
            "sub r13, r14",             // the user's
            "vmovq xmm2, qword ptr [rdi + r10*8 + {t_keep}]",
            "vpinsrq xmm2, xmm2, qword ptr [rdi + r13*8 + {t_keep}], 1",
            "vmovdqu xmm5, xmmword ptr [rdi + r10*8 + {t_range}]",
            "vinserti128 ymm5, ymm5, xmmword ptr [rdi + r13*8 + {t_range}], 1",
            "vpcmpgtb xmm3, xmm0, xmmword ptr [rdi + {c_60}]",     // letters
            "vpand xmm4, xmm3, xmmword ptr [rdi + {c_39}]",
            "vpsubb xmm0, xmm0, xmmword ptr [rdi + {c_30}]",
            "vpsubb xmm0, xmm0, xmm4",                               // digit values
            "vpand xmm3, xmm3, xmmword ptr [rdi + {c_26}]",
            "vpaddb xmm3, xmm3, xmmword ptr [rdi + {c_9}]",       // the most each may be
            "vpsubusb xmm3, xmm0, xmm3",
            "vpand xmm15, xmm3, xmm2",                               // ymm15: what is wrong with the IDs
            "vpand xmm0, xmm0, xmm2",
            "vpmaddubsw xmm0, xmm0, xmmword ptr [rdi + {c_36_1}]",
            "vpmaddwd xmm0, xmm0, xmmword ptr [rdi + {c_1296_1}]",
            "vpmuludq xmm4, xmm0, xmmword ptr [rdi + {c_36p4}]",
            "vpsrlq xmm0, xmm0, 32",
            "vpaddq xmm0, xmm0, xmm4",                               // the two numbers
            "vpermq ymm4, ymm0, 0x50",
            "vpcmpgtq ymm5, ymm5, ymm4",                             // (least > it, bound > it) for each: (no, yes)
            "vpxor ymm5, ymm5, ymmword ptr [rdi + {c_range_ok}]",
            "vpor ymm15, ymm15, ymm5",
            // ---- ts and ingest, with no branch on the form of ts.
            "tzcnt r10, r9",
            "jc 88f",                   // fewer than three commas in sight
            "tzcnt r11, r12",
            "blsr r12, r12",
            "tzcnt r13, r12",
            "sub r10, r11",             // N: the length of ts plus 1
            "shrx r12, rdx, r13",
            "and r12d, 1",              // ingest opens with a quote?
            "lea r11, [rsi + r11 + 1]", // ts
            "lea r13, [rsi + r13 + 1]", // ingest
            "lea rsi, [rsi + r15 + 41]", // rsi: the newline that ends the line
            "mov rax, qword ptr [r11 + 19]",
            "and rax, qword ptr [rdi + r10*8 + {t_fmask}]",
            "or rax, qword ptr [rdi + r10*8 + {t_ftag}]",           // the form's key
            "pext rcx, rax, qword ptr [rdi + {form_bits}]",
            "cmp rax, qword ptr [rdi + rcx*8 + {fkey}]",
            "jne 82f",
            "vmovdqu xmm1, xmmword ptr [r11]",                       // the 13 digits, if that is what ts is
            "cmp r10d, 14",
            "cmove r11, r13",           // date and time: of ts, or of ingest again when ts is digits
            "mov rdx, qword ptr [r11]",
            "mov r8d, dword ptr [r11 + 7]",
            "pext r9d, r8d, dword ptr [rdi + {date_bits}]",          // the date's slot
            "cmp rdx, qword ptr [rdi + r9*8 + {key8}]",
            "jne 83f",
            "cmp r8d, dword ptr [rdi + r9*4 + {key4}]",
            "jne 83f",
            "mov r14, r9",              // ingest's date: nearly always the same bytes
            "cmp rdx, qword ptr [r13]",
            "jne 62f",
            "cmp r8d, dword ptr [r13 + 7]",
            "jne 62f",
            "63:",
            "vmovq xmm3, qword ptr [r11 + 11]",
            "vpinsrq xmm3, xmm3, qword ptr [r13 + 11], 1",
            "vinserti128 ymm1, ymm3, xmm1, 1",                       // two times of day | 13 digits
            "vpxor ymm1, ymm1, ymmword ptr [rdi + {c_tpat}]",
            "vpsubusb ymm2, ymm1, ymmword ptr [rdi + {c_tmax}]",
            "vpmaddubsw ymm1, ymm1, ymmword ptr [rdi + {c_tw1}]",
            "vpcmpgtw ymm3, ymm1, ymmword ptr [rdi + {c_tmaxw}]",
            "vpor ymm2, ymm2, ymm3",
            "vptest ymm2, ymmword ptr [rdi + r10*8 + {t_counts}]",
            "jnz 88f",
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw2}]",
            "vpshufb ymm1, ymm1, ymmword ptr [rdi + {c_tshuf}]",
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw3}]",
            "vpmuludq ymm2, ymm1, ymmword ptr [rdi + {c_tmul}]",
            "vpsrlq ymm1, ymm1, 32",
            "vpaddq ymm1, ymm1, ymm2",
            "vmovq rax, xmm1",          // ts: ms since midnight
            "vpextrq rdx, xmm1, 1",     // ingest: s since midnight
            "vextracti128 xmm1, ymm1, 1",
            "vmovq r8, xmm1",           // the 13 digits' number
            "add rax, qword ptr [rdi + r9*8 + {ms}]",
            "sub rax, qword ptr [rdi + rcx*8 + {fval}]",
            "cmp r10d, 14",
            "cmove rax, r8",            // rax: ts, ms
            "add rdx, qword ptr [rdi + r14*8 + {sec}]",            // rdx: ingest, s
            // ---- The rest of the line, from the comma after ingest.
            "mov ecx, dword ptr [r13 + 19]",
            "and ecx, dword ptr [rdi + r12*4 + {t_zmask}]",
            "cmp ecx, dword ptr [rdi + r12*4 + {t_zpat}]",          // `Z,` or `Z\",`
            "jne 88f",
            "lea r13, [r13 + r12 + 20]",                             // comma 4
            "vmovdqu ymm3, ymmword ptr [rsi - 32]",                  // the 32 bytes before the newline
            "vpcmpeqb ymm4, ymm3, ymmword ptr [rdi + {c_comma}]",
            "vpcmpeqb ymm3, ymm3, ymmword ptr [rdi + {c_quote}]",
            "vpmovmskb ecx, ymm4",
            "vpmovmskb r9d, ymm3",
            "lea r10, [r13 + 32]",
            "sub r10, rsi",             // how far into them comma 4 is
            "cmp r10, 31",
            "ja 88f",                   // more than 32 bytes from comma 4 to the newline (or less than none)
            "shrx rcx, rcx, r10",       // the commas from comma 4 on
            "shrx r9, r9, r10",         // and the quotes
            "mov r8d, 32",
            "sub r8, r10",              // e: from comma 4 to the newline
            "blsr r10d, ecx",
            "tzcnt r11d, r10d",         // k5
            "blsr r10d, r10d",
            "tzcnt r14d, r10d",         // k6
            "blsr r10d, r10d",
            "jnz 88f",                  // more than three commas
            "jc 88f",                   // fewer
            "bts rcx, r8",              // separators: commas 4, 5, 6 and the newline
            "mov r10, r9",
            "shr r10, 1",
            "pext r10, r10, rcx",       // qb: the fields that open with a quote
            "pdep r15, r10, rcx",
            "lea r12, [r10 + r10]",
            "pdep r12, r12, rcx",
            "add r15, r15",             // where their opening quotes are
            "shr r12, 1",               // where their closing quotes must be
            "test r15, r12",
            "jnz 88f",                  // (a lone quote for a field)
            "or r15, r12",
            "cmp r15, r9",
            "jne 88f",                  // a quote missing or anywhere else
            "mov ecx, r10d",
            "and ecx, 1",
            "mov r12d, dword ptr [r13 + rcx + 1]",                   // the type's first four bytes
            "lea ecx, [rcx + rcx + 5]",
            "xor ecx, r11d",            // 0: comma 5 right after four letters
            "mov r15d, r12d",
            "xor r12d, 0x77656976",     // view
            "xor r15d, 0x74726163",     // cart
            "imul r12, r15",
            "or r12, rcx",              // r12: 0 so far for a plain view or cart
            "and r10d, 2",
            "lea ecx, [r10 + r11 + 1]",
            "cmp r14d, ecx",
            "jbe 72f",                  // no product
            "73:",
            // ---- The numbers behind the IDs.
            "vptest ymm15, ymm15",
            "jnz 88f",
            "vmovq rcx, xmm0",
            "vpextrq r8, xmm0, 1",
            "imul rcx, qword ptr [rdi + {binv}]",
            "rorx r10, rcx, 17",
            "xor r10, rcx",
            "shr rcx, 34",
            "xor r10, rcx",
            "shr r10, 24",
            "xor r10, qword ptr [rdi + {ekey}]",
            "imul r10, qword ptr [rdi + {ainv}]",                   // the event's number, top 40 bits
            "imul r8, qword ptr [rdi + {binv}]",
            "rorx r11, r8, 17",
            "xor r11, r8",
            "shr r8, 34",
            "xor r11, r8",
            "shr r11, 24",
            "xor r11, qword ptr [rdi + {ukey}]",
            "imul r11, qword ptr [rdi + {ainv}]",                   // the user's
            "cmp r10, qword ptr [rdi + {ev_top}]",
            "jae 88f",                  // not a dense event
            "pext r8, r10, qword ptr [rdi + {m_stream}]",           // r8: stream
            "pext rcx, r10, qword ptr [rdi + {m_slot2}]",
            "pext r10, r11, qword ptr [rdi + {m_node}]",
            "or rcx, r10",              // rcx: twice the slot | node << 21
            "pext r10, r11, qword ptr [rdi + {m_alias}]",
            "cmp r11, qword ptr [rdi + {us_top}]",
            "sbb r10, -1",              // r10: 0 for a direct user (in bound, alias 0)
            "or r12, r10",
            // ---- A valid view or cart?
            "test r12, r12",
            "jnz 85f",
            "sub rax, qword ptr [rdi + {ws}]",
            "cmp rax, qword ptr [rdi + {span}]",
            "jae 86f",                  // outside the window
            "imul r11, rdx, 1000",
            "sub r11, rax",
            "sub r11, qword ptr [rdi + {xlo}]",
            "cmp r11, qword ptr [rdi + {xspan}]",
            "ja 86f",                   // ingested over a day late (or ages before)
            "add rax, qword ptr [rdi + {wso}]",
            "add rdx, qword ptr [rdi + {rbias}]",
            "shl rdx, 33",
            "lea rdx, [rdx + rax*4]",   // rdx: ingest31 << 33 | ts offset << 2
            "xor eax, eax",
            "99:",
            // ---- Out of line.
            ".pushsection
