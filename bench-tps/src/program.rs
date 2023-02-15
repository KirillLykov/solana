use {
    bincode::serialize,
    solana_sdk::{
        account::{Account, AccountSharedData},
        bpf_loader_upgradeable::UpgradeableLoaderState,
        pubkey::Pubkey,
        rent::Rent,
    },
};

mod spl_instruction_padding {
    solana_sdk::declare_id!("Kqc3iY94x3nJvxYBqxqkqRVL3fgrzibJ4f2K5dUem6L");
}

static SPL_INSTRUCTION_PADDING_PROGRAM: (Pubkey, Pubkey, &[u8]) = (
    spl_instruction_padding::ID,
    solana_sdk::bpf_loader::ID,
    include_bytes!("../tests/fixtures/spl_instruction_padding.so"),
);

pub fn spl_instruction_padding_program(rent: &Rent) -> [(Pubkey, AccountSharedData); 2] {
    let program_data_account;
    let (program_id, loader_id, elf) = &SPL_INSTRUCTION_PADDING_PROGRAM;
    let data = if *loader_id == solana_sdk::bpf_loader_upgradeable::ID {
        let (programdata_address, _) =
            Pubkey::find_program_address(&[program_id.as_ref()], loader_id);
        let mut program_data = bincode::serialize(&UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(Pubkey::default()),
        })
        .unwrap();
        program_data.extend_from_slice(elf);
        program_data_account = (
            programdata_address,
            AccountSharedData::from(Account {
                lamports: rent.minimum_balance(program_data.len()).max(1),
                data: program_data,
                owner: *loader_id,
                executable: false,
                rent_epoch: 0,
            }),
        );
        bincode::serialize(&UpgradeableLoaderState::Program {
            programdata_address,
        })
        .unwrap()
    } else {
        elf.to_vec()
    };
    [
        program_data_account,
        (
            *program_id,
            AccountSharedData::from(Account {
                lamports: rent.minimum_balance(data.len()).max(1),
                data,
                owner: *loader_id,
                executable: true,
                rent_epoch: 0,
            }),
        ),
    ]
}
