        asm!(
            // ---- The masks: newlines 41..104 bytes on; commas and quotes of the first 64.
            "vpcmpeqb ymm6, ymm14, ymmword ptr [rsi + 41]",
            "vpcmpeqb ymm7, ymm14, ymmword ptr [rsi + 73]",
            "vpcmpeqb ymm2, ymm12, ymmword ptr [rsi]",
            "vpcmpeqb ymm3, ymm12, ymmword ptr [rsi + 32]",
            "vpcmpeqb ymm0, ymm13, ymmword ptr [rsi]",
            "vpcmpeqb ymm1, ymm13, ymmword ptr [rsi + 32]",
            "vpmovmskb r15d, ymm6",
            "vpmovmskb r14d, ymm7",
            "vpmovmskb eax, ymm2",
            "vpmovmskb ecx, ymm3",
            "vpmovmskb edx, ymm0",
            "vpmovmskb r8d, ymm1",
            "shl r14, 32",
            "or r15, r14",
            "shl rcx, 32",
            "or rax, rcx",              // commas
            "shl r8, 32",
            "or rdx, r8",               // quotes
            "tzcnt r15, r15",
            "jc 88f",                   // no newline in sight
            "lea r15, [rsi + r15 + 41]", // the newline that ends the line
            "vpcmpeqb ymm4, ymm12, ymmword ptr [r15 - 32]",          // commas and quotes of the 32 bytes before it
            "vpcmpeqb ymm5, ymm13, ymmword ptr [r15 - 32]",
            // ---- Where values end and start.
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
            // ---- The fields.
            "tzcnt r10, r9",
            "tzcnt r11, r12",
            "blsr r9, r9",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r11 + 1], 0x65",
            "jne 88f",
            "vmovq xmm0, qword ptr [rsi + r10 - 8]",
            "sub r10, r11",             // the event ID's length plus 1
            "vmovq xmm2, qword ptr [rdi + r10*8 + {t_keep}]",
            "vmovdqu xmm8, xmmword ptr [rdi + r10*8 + {t_range}]",
            "tzcnt r10, r9",
            "tzcnt r11, r12",
            "blsr r9, r9",
            "blsr r12, r12",
            "cmp byte ptr [rsi + r11 + 1], 0x75",
            "jne 88f",
            "vpinsrq xmm0, xmm0, qword ptr [rsi + r10 - 8], 1",
            "sub r10, r11",             // the user's
            "vpinsrq xmm2, xmm2, qword ptr [rdi + r10*8 + {t_keep}], 1",
            "vinserti128 ymm8, ymm8, xmmword ptr [rdi + r10*8 + {t_range}], 1",
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
            "mov rsi, r15",             // rsi: the newline, where the next pass starts
            // ---- ts and ingest: what the memos know.
            "mov rax, qword ptr [r11 + 19]",
            "and rax, qword ptr [rdi + r10*8 + {t_fmask}]",
            "or rax, qword ptr [rdi + r10*8 + {t_ftag}]",           // the form's key
            "pext rcx, rax, qword ptr [rdi + {form_bits}]",
            "cmp rax, qword ptr [rdi + rcx*8 + {fkey}]",
            "jne 82f",
            "vmovdqu xmm1, xmmword ptr [r11]",                       // the 13 digits, if that is what ts is
            "vmovdqu ymm10, ymmword ptr [rdi + r10*8 + {t_counts}]",
            "vmovdqu ymm11, ymmword ptr [rdi + r10*8 + {t_sel}]",
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
            "vmovq xmm9, qword ptr [rdi + r9*8 + {ms}]",
            "vmovq xmm6, qword ptr [rdi + rcx*8 + {fval}]",
            "vpsubq xmm9, xmm9, xmm6",
            "vpinsrq xmm9, xmm9, qword ptr [rdi + r14*8 + {sec}], 1", // (date of ts - form's offset, ingest's date)
            "mov eax, dword ptr [r13 + 19]",
            "and eax, dword ptr [rdi + r12*4 + {t_zmask}]",
            "cmp eax, dword ptr [rdi + r12*4 + {t_zpat}]",          // `Z,` or `Z\",`
            "jne 88f",
            "lea r13, [r13 + r12 + 20]",                             // comma 4
            // ---- Digits of the IDs; the times; the type.
            "vpcmpgtb xmm3, xmm0, xmmword ptr [rdi + {c_60}]",     // letters
            "vpand xmm6, xmm3, xmmword ptr [rdi + {c_39}]",
            "vpsubb xmm0, xmm0, xmmword ptr [rdi + {c_30}]",
            "vpsubb xmm0, xmm0, xmm6",                               // digit values
            "vpand xmm3, xmm3, xmmword ptr [rdi + {c_26}]",
            "vpaddb xmm3, xmm3, xmmword ptr [rdi + {c_9}]",       // the most each may be
            "vpsubusb xmm3, xmm0, xmm3",
            "vpand xmm15, xmm3, xmm2",                               // ymm15: what is wrong with the IDs
            "vpand xmm0, xmm0, xmm2",
            "vpxor ymm1, ymm1, ymmword ptr [rdi + {c_tpat}]",
            "vpsubusb ymm3, ymm1, ymmword ptr [rdi + {c_tmax}]",
            "vpmaddubsw ymm1, ymm1, ymmword ptr [rdi + {c_tw1}]",
            "vpcmpgtw ymm6, ymm1, ymmword ptr [rdi + {c_tmaxw}]",
            "vpor ymm3, ymm3, ymm6",
            "vptest ymm3, ymm10",
            "jnz 88f",
            "vpbroadcastq ymm6, qword ptr [r13]",
            "vpand ymm6, ymm6, ymmword ptr [rdi + {c_typemask}]",
            "vpcmpeqq ymm6, ymm6, ymmword ptr [rdi + {c_typepat}]", // `,view,` `,cart,` `,\"view\",` `,\"cart\",`
            "vmovmskpd r12d, ymm6",
            "cmp r12d, 1",
            "sbb r12, r12",             // r12: 0 for a view or cart, else all ones
            // ---- The rest of the line: from comma 4 to the newline.
            "vpmovmskb ecx, ymm4",
            "vpmovmskb r9d, ymm5",
            "lea r10, [r13 + 32]",
            "sub r10, rsi",             // how far into the 32 bytes comma 4 is
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
            "lea rax, [r10 + r10]",
            "pdep rax, rax, rcx",
            "add r15, r15",             // where their opening quotes are
            "shr rax, 1",               // where their closing quotes must be
            "test r15, rax",
            "jnz 88f",                  // (a lone quote for a field)
            "or r15, rax",
            "cmp r15, r9",
            "jne 88f",                  // a quote missing or anywhere else
            "and r10d, 2",
            "lea ecx, [r10 + r11 + 1]",
            "cmp r14d, ecx",
            "jbe 72f",                  // no product
            "73:",
            // ---- The IDs' numbers; the times.
            "vpmaddubsw xmm0, xmm0, xmmword ptr [rdi + {c_36_1}]",
            "vpmaddwd xmm0, xmm0, xmmword ptr [rdi + {c_1296_1}]",
            "vpmuludq xmm6, xmm0, xmmword ptr [rdi + {c_36p4}]",
            "vpsrlq xmm0, xmm0, 32",
            "vpaddq xmm0, xmm0, xmm6",                               // the two numbers
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw2}]",
            "vpshufb ymm1, ymm1, ymmword ptr [rdi + {c_tshuf}]",
            "vpmaddwd ymm1, ymm1, ymmword ptr [rdi + {c_tw3}]",
            "vpmuludq ymm3, ymm1, ymmword ptr [rdi + {c_tmul}]",
            "vpsrlq ymm1, ymm1, 32",
            "vpaddq ymm1, ymm1, ymm3",                               // ts ms of day, ingest s of day | the digits' number
            "vpaddq ymm1, ymm1, ymm9",                               // with the dates, less the form's offset
            "vpand ymm1, ymm1, ymm11",                               // the one ts has: by date and time, or the digits
            "vextracti128 xmm3, ymm1, 1",
            "vpaddq xmm1, xmm1, xmm3",                               // (ts in ms, ingest in s)
            "vpermq ymm6, ymm0, 0x50",
            "vpcmpgtq ymm8, ymm8, ymm6",                             // (least > it, bound > it) for each: (no, yes)
            "vpxor ymm8, ymm8, ymmword ptr [rdi + {c_range_ok}]",
            "vpor ymm15, ymm15, ymm8",
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
            "vmovq rax, xmm1",          // rax: ts, ms
            "vpextrq rdx, xmm1, 1",     // rdx: ingest, s
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
            ".pushsection
